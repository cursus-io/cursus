package controller

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

type LocalProcessor interface {
	ProcessCommand(cmd string) string
}

type ClusterRouter struct {
	mu             sync.RWMutex
	LocalAddr      string
	brokerID       string
	rm             RaftManager
	clientPort     int
	clientHost     string
	internalPort   int
	internalTLS    *tls.Config
	internalToken  string
	timeout        time.Duration
	localProcessor LocalProcessor

	// Cached coordinator ring
	coordRing       *util.ConsistentHashRing
	coordBrokerHash string // hash of active broker IDs to detect changes
}

func NewClusterRouter(brokerID, localAddr string, processor LocalProcessor, rm RaftManager, clientPort int, clientHost string, cfg *config.Config) *ClusterRouter {
	internalPort := 0
	var internalTLS *tls.Config
	internalToken := ""
	if cfg != nil {
		internalPort = cfg.InternalBrokerPort
		internalTLS = cfg.InternalClientTLSConfig()
		internalToken = cfg.InternalAuthToken
	}
	return &ClusterRouter{
		brokerID:       brokerID,
		LocalAddr:      localAddr,
		rm:             rm,
		clientPort:     clientPort,
		clientHost:     clientHost,
		internalPort:   internalPort,
		internalTLS:    internalTLS,
		internalToken:  internalToken,
		timeout:        5 * time.Second,
		localProcessor: processor,
	}
}

func (r *ClusterRouter) BrokerID() string {
	return r.brokerID
}

func (r *ClusterRouter) ClientPort() int {
	return r.clientPort
}

func (r *ClusterRouter) getLeader() (string, error) {
	leader := r.rm.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no leader available from Raft")
	}
	return leader, nil
}

func (r *ClusterRouter) ForwardToLeader(req string) (string, error) {
	leader, err := r.getLeader()
	if r.rm.IsLeader() {
		return r.processLocally(req), nil
	}

	if err != nil || leader == "" {
		return "", fmt.Errorf("leader unknown, cannot process command: %w", err)
	}

	if leader == r.LocalAddr {
		return "", fmt.Errorf("node marked as leader in Raft but IsLeader() is false (transitioning?)")
	}

	return r.forwardWithTimeout(leader, req)
}

func (r *ClusterRouter) ForwardToPartitionLeader(topic string, partition int, req string) (string, error) {
	fsm := r.rm.GetFSM()
	if fsm == nil {
		return r.ForwardToLeader(req)
	}

	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return r.ForwardToLeader(req)
	}

	if meta.Leader == r.brokerID {
		return r.processLocally(req), nil
	}

	broker := fsm.GetBroker(meta.Leader)
	if broker == nil {
		return "", fmt.Errorf("partition leader broker %s not found in registry", meta.Leader)
	}

	return r.forwardWithTimeout(broker.Addr, req)
}

func (r *ClusterRouter) FindCoordinator(groupName string) (string, string, error) {
	routes, err := r.coordinatorRoutes([]string{groupName})
	if err != nil {
		return "", "", err
	}
	route, ok := routes[groupName]
	if !ok {
		return "", "", fmt.Errorf("coordinator route for group %q not found", groupName)
	}
	return route.id, route.addr, nil
}

// FindCoordinatorOwners resolves a set of groups from one broker-membership
// snapshot. It is used by scrape-time observation to avoid rebuilding and
// validating the coordinator ring once per group.
func (r *ClusterRouter) FindCoordinatorOwners(groupNames []string) (map[string]string, error) {
	routes, err := r.coordinatorRoutes(groupNames)
	if err != nil {
		return nil, err
	}
	owners := make(map[string]string, len(routes))
	for groupName, route := range routes {
		owners[groupName] = route.id
	}
	return owners, nil
}

type coordinatorRoute struct {
	id   string
	addr string
}

func (r *ClusterRouter) coordinatorRoutes(groupNames []string) (map[string]coordinatorRoute, error) {
	if r == nil || r.rm == nil {
		return nil, fmt.Errorf("raft manager not available")
	}
	fsmRef := r.rm.GetFSM()
	if fsmRef == nil {
		return nil, fmt.Errorf("FSM not available")
	}

	brokers := fsmRef.GetBrokers()
	activeBrokerIDs := make([]string, 0, len(brokers))
	activeBrokerAddrs := make(map[string]string, len(brokers))
	for _, info := range brokers {
		if info.Status == "active" {
			activeBrokerIDs = append(activeBrokerIDs, info.ID)
			activeBrokerAddrs[info.ID] = info.Addr
		}
	}

	if len(activeBrokerIDs) == 0 {
		return nil, fmt.Errorf("no active brokers available")
	}

	sort.Strings(activeBrokerIDs)
	brokerHash := strings.Join(activeBrokerIDs, ",")

	// Check if rebuild needed
	r.mu.RLock()
	needsRebuild := r.coordRing == nil || r.coordBrokerHash != brokerHash
	r.mu.RUnlock()

	if needsRebuild {
		r.mu.Lock()
		// Double-check
		if r.coordRing == nil || r.coordBrokerHash != brokerHash {
			r.coordRing = util.NewConsistentHashRing(150, nil)
			r.coordRing.Add(activeBrokerIDs...)
			r.coordBrokerHash = brokerHash
		}
		r.mu.Unlock()
	}

	routes := make(map[string]coordinatorRoute, len(groupNames))
	r.mu.RLock()
	for _, groupName := range groupNames {
		coordID := r.coordRing.Get(groupName)
		addr, ok := activeBrokerAddrs[coordID]
		if !ok {
			r.mu.RUnlock()
			return nil, fmt.Errorf("coordinator broker %s not found in active registry", coordID)
		}
		routes[groupName] = coordinatorRoute{id: coordID, addr: addr}
	}
	r.mu.RUnlock()

	return routes, nil
}

func (r *ClusterRouter) ForwardToCoordinator(groupName, req string) (string, error) {
	id, addr, err := r.FindCoordinator(groupName)
	if err != nil {
		return "", err
	}

	if id == r.brokerID {
		return r.processLocally(req), nil
	}

	return r.forwardWithTimeout(addr, req)
}

func (r *ClusterRouter) forwardWithTimeout(addr, req string) (string, error) {
	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return "", fmt.Errorf("invalid address format %s: %w", addr, splitErr)
	}

	clientAddr := r.brokerCommandAddr(host)
	resp, err := r.sendRequest(clientAddr, req)
	if err != nil {
		return "", fmt.Errorf("failed to forward request to %s: %w", clientAddr, err)
	}
	return resp, nil
}

func (r *ClusterRouter) ForwardDataToLeader(data []byte) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if r.rm.IsLeader() || leader == r.LocalAddr {
		return "", fmt.Errorf("internal routing error: cannot forward batch data to self")
	}

	return r.forwardDataWithTimeout(leader, data)
}

func (r *ClusterRouter) ForwardDataToPartitionLeader(topic string, partition int, data []byte) (string, error) {
	fsm := r.rm.GetFSM()
	if fsm == nil {
		return r.ForwardDataToLeader(data)
	}

	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return r.ForwardDataToLeader(data)
	}

	if meta.Leader == r.brokerID {
		return "", fmt.Errorf("internal routing error: current node is leader for partition %s", partitionKey)
	}

	broker := fsm.GetBroker(meta.Leader)
	if broker == nil {
		return "", fmt.Errorf("partition leader broker %s not found in registry", meta.Leader)
	}

	return r.forwardDataWithTimeout(broker.Addr, data)
}

func (r *ClusterRouter) forwardDataWithTimeout(addr string, data []byte) (string, error) {
	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return "", fmt.Errorf("invalid address format %s: %w", addr, splitErr)
	}

	clientAddr := r.brokerCommandAddr(host)
	return r.sendDataRequest(clientAddr, r.wrapInternalBatch(data))
}

func (r *ClusterRouter) brokerCommandAddr(host string) string {
	port := r.clientPort
	if r.internalPort > 0 {
		port = r.internalPort
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (r *ClusterRouter) processLocally(req string) string {
	if r.localProcessor != nil {
		return r.localProcessor.ProcessCommand(req)
	}
	return "ERROR: local_processor_not_configured"
}

func (r *ClusterRouter) withInternalToken(command string) string {
	if r.internalToken == "" {
		return command
	}
	if wireprotocol.IsTextCommand(command) {
		return injectInternalToken(command, r.internalToken)
	}
	if _, payload, err := util.DecodeMessage([]byte(command)); err == nil {
		return string(util.EncodeMessage("", injectInternalToken(payload, r.internalToken)))
	}
	return injectInternalToken(command, r.internalToken)
}

func injectInternalToken(command, token string) string {
	trimmed := strings.TrimSpace(command)
	if trimmed == "" {
		return command
	}
	commandEnd := strings.IndexAny(trimmed, " \t\r\n")
	if commandEnd == -1 {
		return trimmed + " internal_token=" + token
	}
	commandName := trimmed[:commandEnd]
	rest := strings.TrimLeft(trimmed[commandEnd:], " \t\r\n")
	if rest == "" {
		return commandName + " internal_token=" + token
	}
	firstArgEnd := strings.IndexAny(rest, " \t\r\n")
	firstArg := rest
	if firstArgEnd >= 0 {
		firstArg = rest[:firstArgEnd]
	}
	if strings.HasPrefix(firstArg, "internal_token=") {
		return command
	}
	return commandName + " internal_token=" + token + " " + rest
}

func (r *ClusterRouter) wrapInternalBatch(data []byte) []byte {
	if r.internalToken == "" {
		return data
	}
	payload := base64.StdEncoding.EncodeToString(data)
	return []byte("INTERNAL_BATCH internal_token=" + r.internalToken + " payload=" + payload)
}
func (r *ClusterRouter) sendRequest(addr, command string) (string, error) {
	return r.sendDataRequest(addr, []byte(r.withInternalToken(command)))
}

func (r *ClusterRouter) sendDataRequest(addr string, data []byte) (string, error) {
	var conn net.Conn
	var err error
	if r.internalTLS != nil {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{}, Config: r.internalTLS}
		conn, err = tlsDialer.DialContext(ctx, "tcp", addr)
	} else {
		conn, err = (&net.Dialer{Timeout: r.timeout}).Dial("tcp", addr)
	}
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return "", err
	}

	connection, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return "", fmt.Errorf("negotiate internal Wire v2 connection: %w", err)
	}
	command := wire.CommandPublish
	if !wire.IsBatch(data) {
		var request wire.CommandPayload
		command, request, err = wire.ParseCommandText(string(data))
		if err != nil {
			return "", err
		}
		data, err = wire.EncodeCommandPayload(request)
		if err != nil {
			return "", err
		}
	}
	const requestID = 1
	if err := connection.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: command, RequestID: requestID, Payload: data}); err != nil {
		return "", fmt.Errorf("write internal Wire v2 request: %w", err)
	}
	response, err := connection.ReadFrame()
	if err != nil {
		return "", fmt.Errorf("read internal Wire v2 response: %w", err)
	}
	if response.RequestID != requestID || response.Command != command {
		return "", fmt.Errorf("internal Wire v2 response correlation mismatch")
	}
	return string(response.Payload), nil
}
