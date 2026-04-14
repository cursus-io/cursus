package controller

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
)

type LocalProcessor interface {
	ProcessCommand(cmd string) string
}

type ClusterRouter struct {
	LocalAddr      string
	brokerID       string
	rm             RaftManager
	clientPort     int
	timeout        time.Duration
	localProcessor LocalProcessor

	// Cached coordinator ring
	coordRing       *util.ConsistentHashRing
	coordBrokerHash string // hash of active broker IDs to detect changes
}

func NewClusterRouter(brokerID, localAddr string, processor LocalProcessor, rm RaftManager, clientPort int) *ClusterRouter {
	return &ClusterRouter{
		brokerID:       brokerID,
		LocalAddr:      localAddr,
		rm:             rm,
		clientPort:     clientPort,
		timeout:        5 * time.Second,
		localProcessor: processor,
	}
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

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
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
	fsmRef := r.rm.GetFSM()
	if fsmRef == nil {
		return "", "", fmt.Errorf("FSM not available")
	}

	brokers := fsmRef.GetBrokers()
	var activeBrokerIDs []string
	for _, info := range brokers {
		if info.Status == "active" {
			activeBrokerIDs = append(activeBrokerIDs, info.ID)
		}
	}

	if len(activeBrokerIDs) == 0 {
		return "", "", fmt.Errorf("no active brokers available")
	}

	sort.Strings(activeBrokerIDs)
	brokerHash := strings.Join(activeBrokerIDs, ",")

	// Rebuild ring only when broker membership changes
	if r.coordRing == nil || r.coordBrokerHash != brokerHash {
		r.coordRing = util.NewConsistentHashRing(150, nil)
		r.coordRing.Add(activeBrokerIDs...)
		r.coordBrokerHash = brokerHash
	}

	coordID := r.coordRing.Get(groupName)
	broker := fsmRef.GetBroker(coordID)
	if broker == nil {
		return "", "", fmt.Errorf("coordinator broker %s not found in registry", coordID)
	}

	return coordID, broker.Addr, nil
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

	clientAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
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

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
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

	clientAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	return r.sendDataRequest(clientAddr, data)
}

func (r *ClusterRouter) processLocally(req string) string {
	if r.localProcessor != nil {
		return r.localProcessor.ProcessCommand(req)
	}
	return "ERROR: no local processor configured"
}

func (r *ClusterRouter) sendRequest(addr, command string) (string, error) {
	return r.sendDataRequest(addr, []byte(command))
}

func (r *ClusterRouter) sendDataRequest(addr string, data []byte) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return "", err
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		return "", fmt.Errorf("failed to write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return "", fmt.Errorf("failed to write data: %w", err)
	}

	respLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLenBuf); err != nil {
		return "", fmt.Errorf("failed to read response length: %w", err)
	}

	respLen := binary.BigEndian.Uint32(respLenBuf)
	if respLen == 0 {
		return "", nil
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return "", fmt.Errorf("failed to read full response body: %w", err)
	}

	return string(respBuf), nil
}
