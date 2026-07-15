package controller

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"

	"github.com/google/uuid"
)

const DefaultFSMApplyTimeout = 5 * time.Second

func backoffDelay(attempt int, base time.Duration) time.Duration {
	if attempt > 30 {
		attempt = 30
	}
	delay := base * time.Duration(1<<uint(attempt))
	if delay > 5*time.Second {
		delay = 5 * time.Second
	}
	// Add jitter: ±25%
	jitter := time.Duration(rand.Int63n(int64(delay)/2)) - delay/4
	return delay + jitter
}

// isDistributed returns true if the broker is running in distributed cluster mode.
func (ch *CommandHandler) isDistributed() bool {
	return ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil
}

// hasRouter returns true if distributed mode is enabled with a working router.
func (ch *CommandHandler) hasRouter() bool {
	return ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.Router != nil
}

func (ch *CommandHandler) ProcessCommand(cmd string) string {
	ctx := NewInternalClientContext("default-group", 0)

	// Forwarded commands arrive wrapped in a binary envelope
	// (2-byte topic length + topic + payload). Decode it first.
	if _, payload, err := util.DecodeMessage([]byte(cmd)); err == nil {
		cmd = strings.TrimSpace(payload)
	}

	return ch.HandleCommand(cmd, ctx)
}

func (ch *CommandHandler) isAuthorizedForPartition(topic string, partition int) bool {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil {
		return true
	}
	return ch.Cluster.IsAuthorized(topic, partition)
}

// isLeaderAndForward checks if the current node is the cluster leader
func (ch *CommandHandler) isLeaderAndForward(cmd string) (string, bool, error) {
	if !ch.isDistributed() {
		return "", false, nil
	}

	const maxRetries = 5

	for i := 0; i < maxRetries; i++ {
		if ch.Cluster.RaftManager.GetLeaderAddress() != "" {
			break
		}
		if i == maxRetries-1 {
			return "ERROR: no_raft_leader", true, fmt.Errorf("no leader elected")
		}
		util.Debug("Waiting for Raft leader to be elected... (attempt %d/%d)", i+1, maxRetries)
		time.Sleep(backoffDelay(i, 100*time.Millisecond))
	}

	if !ch.Cluster.RaftManager.IsLeader() {
		if ch.Cluster.Router == nil {
			return "ERROR: router_not_available", true, nil
		}

		encodedCmd := util.EncodeMessage("", cmd)
		var lastErr error
		for i := 0; i < maxRetries; i++ {
			resp, err := ch.Cluster.Router.ForwardToLeader(string(encodedCmd))
			if err == nil {
				return resp, true, nil
			}
			lastErr = err
			util.Debug("Retrying forward to leader (Target Leader %s)... attempt %d: %v", ch.Cluster.RaftManager.GetLeaderAddress(), i+1, err)
			if i < maxRetries-1 {
				time.Sleep(backoffDelay(i, 100*time.Millisecond))
			}
		}
		leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
		return fmt.Sprintf("ERROR: forward_to_leader_failed leader=%s reason=%q", leaderAddr, lastErr.Error()), true, nil
	}
	return "", false, nil
}

// AdvertisedAddr holds the external-facing address for a coordinator broker.
type AdvertisedAddr struct {
	Host string
	Port int
}

type coordCacheEntry struct {
	addr    AdvertisedAddr
	updated time.Time
}

const coordCacheTTL = 30 * time.Second

// checkCoordinator checks if this broker is the coordinator for the given group.
// Returns (addr, false) if another broker is coordinator, or (_, true) if we are.
func (ch *CommandHandler) checkCoordinator(groupName string) (AdvertisedAddr, bool) {
	return ch.checkCoordinatorKey(groupName, fmt.Sprintf("FIND_COORDINATOR group=%s", groupName))
}

func (ch *CommandHandler) checkTransactionCoordinator(txnID string) (AdvertisedAddr, bool) {
	return ch.checkCoordinatorKey(transactionCoordinatorKey(txnID), fmt.Sprintf("FIND_COORDINATOR transactional_id=%s", txnID))
}

func (ch *CommandHandler) checkCoordinatorKey(coordKey string, findCmd string) (AdvertisedAddr, bool) {
	if !ch.hasRouter() {
		return AdvertisedAddr{}, true
	}
	id, raftAddr, err := ch.Cluster.Router.FindCoordinator(coordKey)
	if err != nil {
		return AdvertisedAddr{}, true
	}
	if id == ch.Cluster.Router.BrokerID() {
		return AdvertisedAddr{}, true
	}

	ch.coordCacheMu.RLock()
	if cached, ok := ch.coordCache[id]; ok && time.Since(cached.updated) < coordCacheTTL {
		ch.coordCacheMu.RUnlock()
		return cached.addr, false
	}
	ch.coordCacheMu.RUnlock()

	if fsm := ch.Cluster.RaftManager.GetFSM(); fsm != nil {
		if broker := fsm.GetBroker(id); broker != nil && broker.ClientAddr != "" {
			if host, portStr, splitErr := net.SplitHostPort(broker.ClientAddr); splitErr == nil {
				if port, scanErr := strconv.Atoi(portStr); scanErr == nil && host != "" && port > 0 {
					addr := AdvertisedAddr{Host: host, Port: port}
					ch.coordCacheMu.Lock()
					ch.coordCache[id] = coordCacheEntry{addr: addr, updated: time.Now()}
					ch.coordCacheMu.Unlock()
					return addr, false
				}
			}
		}
	}

	encodedCmd := util.EncodeMessage("", findCmd)
	resp, fwdErr := ch.Cluster.Router.ForwardToCoordinator(coordKey, string(encodedCmd))
	if fwdErr == nil && strings.HasPrefix(resp, "OK") {
		host, port := "", 0
		for _, part := range strings.Fields(resp) {
			if strings.HasPrefix(part, "host=") {
				host = strings.TrimPrefix(part, "host=")
			} else if strings.HasPrefix(part, "port=") {
				_, _ = fmt.Sscanf(part, "port=%d", &port)
			}
		}
		if host != "" && port > 0 {
			addr := AdvertisedAddr{Host: host, Port: port}
			ch.coordCacheMu.Lock()
			ch.coordCache[id] = coordCacheEntry{addr: addr, updated: time.Now()}
			ch.coordCacheMu.Unlock()
			return addr, false
		}
	}

	host := ch.Config.AdvertisedClientHost
	if host == "" {
		host, _, _ = net.SplitHostPort(raftAddr)
	}
	if host == "" {
		host = "localhost"
	}
	return AdvertisedAddr{Host: host, Port: ch.Cluster.Router.ClientPort()}, false
}

// notCoordinatorResponse builds the NOT_COORDINATOR error response.
func notCoordinatorResponse(addr AdvertisedAddr) string {
	return fmt.Sprintf("ERROR: NOT_COORDINATOR host=%s port=%d", addr.Host, addr.Port)
}

func (ch *CommandHandler) isPartitionLeaderAndForward(topic string, partition int, cmd string) (string, bool, error) {
	if !ch.hasRouter() {
		return "", false, nil
	}

	// Fast-path: Check if we are already the owner
	if ch.Cluster.IsAuthorized(topic, partition) {
		return "", false, nil
	}

	const maxRetries = 3

	encodedCmd := util.EncodeMessage("", cmd)
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		resp, err := ch.Cluster.Router.ForwardToPartitionLeader(topic, partition, string(encodedCmd))
		if err == nil {
			if !strings.HasPrefix(resp, "ERROR: not the partition leader") {
				return resp, true, nil
			}
			lastErr = fmt.Errorf("%s", resp)
		} else {
			lastErr = err
		}
		time.Sleep(backoffDelay(i, 100*time.Millisecond))
	}

	return fmt.Sprintf("ERROR: forward_to_partition_leader_failed topic=%s partition=%d reason=%q", topic, partition, lastErr.Error()), true, nil
}

// resolvePartitionLeaderAddr returns the client-facing address of the partition leader.
func (ch *CommandHandler) resolvePartitionLeaderAddr(topicName string, partitionID int) string {
	if !ch.isDistributed() {
		return ch.fallbackClientAddr()
	}

	fsmRef := ch.Cluster.RaftManager.GetFSM()
	if fsmRef == nil {
		return ch.fallbackClientAddr()
	}

	partitionKey := fmt.Sprintf("%s-%d", topicName, partitionID)
	meta := fsmRef.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return ch.fallbackClientAddr()
	}

	broker := fsmRef.GetBroker(meta.Leader)
	if broker == nil {
		return ch.fallbackClientAddr()
	}

	if broker.ClientAddr != "" {
		return broker.ClientAddr
	}

	// Legacy fallback: derive from Raft addr
	if h, _, err := net.SplitHostPort(broker.Addr); err == nil {
		return fmt.Sprintf("%s:%d", h, ch.Cluster.Router.ClientPort())
	}
	return ch.fallbackClientAddr()
}

func (ch *CommandHandler) fallbackClientAddr() string {
	host := ch.Config.AdvertisedClientHost
	if host == "" {
		host = "localhost"
	}
	port := ch.Config.AdvertisedBrokerPort
	if port == 0 {
		port = ch.Config.BrokerPort
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// handleRaftApply processes RAFT_APPLY command (internal, from coordinator to leader).
func (ch *CommandHandler) handleRaftApply(cmd string) string {
	rest := cmd[11:] // len("RAFT_APPLY ") = 11

	typeIdx := strings.Index(rest, "type=")
	payloadIdx := strings.Index(rest, "payload=")
	if typeIdx == -1 || payloadIdx == -1 {
		return "ERROR: missing_required_params command=RAFT_APPLY params=type,payload"
	}

	cmdType := strings.TrimSpace(rest[typeIdx+5 : payloadIdx])
	payloadStr := strings.TrimSpace(rest[payloadIdx+8:])

	if cmdType == "" || payloadStr == "" {
		return "ERROR: empty_required_params command=RAFT_APPLY params=type,payload"
	}

	if !ch.isDistributed() {
		return "ERROR: distribution_required command=RAFT_APPLY"
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return fmt.Sprintf("ERROR: invalid_payload_json reason=%q", err.Error())
	}

	_, err := ch.applyAndWait(cmdType, payload)
	if err != nil {
		return fmt.Sprintf("ERROR: raft_apply_failed reason=%q", err.Error())
	}
	return "OK"
}

// applyViaLeader tries local Raft apply first; if not leader, forwards to leader via RAFT_APPLY.
func (ch *CommandHandler) applyViaLeader(cmdType string, payload map[string]interface{}) (interface{}, error) {
	result, err := ch.applyAndWait(cmdType, payload)
	if err == nil {
		return result, nil
	}

	if !ch.isDistributed() || ch.Cluster.Router == nil {
		return nil, err
	}

	data, jsonErr := json.Marshal(payload)
	if jsonErr != nil {
		return nil, fmt.Errorf("marshal payload: %w", jsonErr)
	}

	forwardCmd := fmt.Sprintf("RAFT_APPLY %stype=%s payload=%s", ch.internalAuthPrefix(), cmdType, string(data))
	encodedCmd := util.EncodeMessage("", forwardCmd)
	resp, fwdErr := ch.Cluster.Router.ForwardToLeader(string(encodedCmd))
	if fwdErr != nil {
		return nil, fmt.Errorf("forward raft apply to leader: %w", fwdErr)
	}
	if strings.HasPrefix(resp, "ERROR") {
		return nil, fmt.Errorf("leader raft apply: %s", resp)
	}
	return resp, nil
}

func (ch *CommandHandler) applyAndWait(cmdType string, payload map[string]interface{}) (interface{}, error) {
	if ch.Cluster == nil {
		return nil, fmt.Errorf("cluster controller is not initialized")
	}
	if ch.Cluster.RaftManager == nil {
		return nil, fmt.Errorf("raft manager not available")
	}
	fsm := ch.Cluster.RaftManager.GetFSM()
	if fsm == nil {
		return nil, fmt.Errorf("fsm not available")
	}

	reqID := uuid.New().String()
	payload["req_id"] = reqID

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	respChan := fsm.RegisterNotifier(reqID)
	defer fsm.UnregisterNotifier(reqID)

	err = ch.Cluster.RaftManager.ApplyCommand(cmdType, data)
	if err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	select {
	case res := <-respChan:
		if err, ok := res.(error); ok && err != nil {
			return nil, err
		}
		return res, nil
	case <-time.After(DefaultFSMApplyTimeout):
		return nil, fmt.Errorf("timeout waiting for FSM")
	}
}
