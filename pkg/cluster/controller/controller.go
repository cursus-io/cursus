package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

// LeaderChecker provides leadership status queries.
type LeaderChecker interface {
	IsLeader() bool
	GetLeaderAddress() string
	LeaderCh() <-chan bool
}

// CommandApplier applies commands to the Raft log.
type CommandApplier interface {
	ApplyCommand(prefix string, data []byte) error
	ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error)
}

// MembershipManager handles cluster membership changes.
type MembershipManager interface {
	AddVoter(id string, addr string) error
	RemoveServer(id string) error
	GetConfiguration() raft.ConfigurationFuture
}

// FSMAccessor provides access to the finite state machine.
type FSMAccessor interface {
	GetFSM() *fsm.BrokerFSM
}

// Replicator handles message replication with quorum.
type Replicator interface {
	ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error)
	ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error)
}

// ISRProvider provides access to the ISR manager.
type ISRProvider interface {
	GetISRManager() replication.ISRManagerInterface
}

// RaftManager is the composite interface for full Raft functionality.
// Individual components should depend on the narrowest sub-interface they need.
type RaftManager interface {
	LeaderChecker
	CommandApplier
	MembershipManager
	FSMAccessor
	Replicator
	ISRProvider
}

type ClusterController struct {
	RaftManager RaftManager
	Discovery   ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
	Config      *config.Config
	brokerID    string
}

func NewClusterController(ctx context.Context, cfg *config.Config, rm RaftManager, sd ServiceDiscovery, brokerID, localAddr string) *ClusterController {
	cc := &ClusterController{
		RaftManager: rm,
		Discovery:   sd,
		Election:    NewControllerElection(rm),
		Router:      NewClusterRouter(brokerID, localAddr, nil, rm, cfg.BrokerPort, cfg.AdvertisedClientHost, cfg),
		Config:      cfg,
		brokerID:    brokerID,
	}

	return cc
}

func (cc *ClusterController) Start(ctx context.Context) {
	cc.Election.Start()
	cc.Discovery.StartReconciler(ctx)
}

func (cc *ClusterController) SetLocalProcessor(lp LocalProcessor) {
	if lp == nil {
		util.Warn("LocalProcessor is nil, ignoring")
		return
	}
	if cc.Router != nil {
		cc.Router.localProcessor = lp
	}
}

func (cc *ClusterController) GetClusterLeader() (string, error) {
	leader := cc.RaftManager.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no cluster leader available")
	}
	return leader, nil
}

func (cc *ClusterController) JoinNewBroker(id, addr string) error {
	_, err := cc.Discovery.AddNode(id, addr)
	return err
}

func (cc *ClusterController) IsLeader() bool {
	if cc.RaftManager != nil {
		return cc.RaftManager.IsLeader()
	}
	util.Warn("RaftManager is nil, assuming non-leader state")
	return false
}

func (cc *ClusterController) IsAuthorized(topic string, partition int) bool {
	if cc.RaftManager == nil {
		return false
	}

	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return false
	}

	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return false
	}

	return meta.Leader == cc.brokerID
}

func (cc *ClusterController) internalAuthPrefix() string {
	if cc != nil && cc.Config != nil && cc.Config.InternalAuthToken != "" {
		return "internal_token=" + cc.Config.InternalAuthToken + " "
	}
	return ""
}

func (cc *ClusterController) ForwardCommandToBroker(addr, command string) (string, error) {
	if cc.Router == nil {
		return "", fmt.Errorf("cluster router not available")
	}
	return cc.Router.forwardWithTimeout(addr, command)
}

func (cc *ClusterController) ReplicateCommandToFollowers(topic string, partition int, command string, minISR int) error {
	replicationStart := time.Now()

	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return fmt.Errorf("FSM not available")
	}

	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return fmt.Errorf("partition metadata not found")
	}

	targets := []string{}
	for _, replica := range meta.Replicas {
		if replica != cc.brokerID {
			targets = append(targets, replica)
		}
	}

	var wg sync.WaitGroup
	var successCount int32 = 1
	var mu sync.Mutex
	errCh := make(chan error, len(targets))

	partitionStr := fmt.Sprintf("%d", partition)
	for _, targetID := range targets {
		broker := fsm.GetBroker(targetID)
		if broker == nil {
			continue
		}

		wg.Add(1)
		go func(addr, brokerID string) {
			defer wg.Done()
			resp, err := cc.Router.forwardWithTimeout(addr, command)
			if err == nil && !strings.HasPrefix(resp, "ERROR") {
				mu.Lock()
				successCount++
				mu.Unlock()
				metrics.ClusterReplicationLag.WithLabelValues(topic, partitionStr, brokerID).Observe(time.Since(replicationStart).Seconds())
				if isrMgr := cc.RaftManager.GetISRManager(); isrMgr != nil {
					isrMgr.UpdateHeartbeat(brokerID)
				}
				return
			}
			if err != nil {
				errCh <- err
			} else {
				errCh <- fmt.Errorf("replica command failed: %s", resp)
			}
		}(broker.Addr, targetID)
	}

	wg.Wait()
	close(errCh)

	if int(successCount) < minISR {
		var reasons []string
		for err := range errCh {
			reasons = append(reasons, err.Error())
		}
		return fmt.Errorf("insufficient successful acknowledgements: got %d, want minISR %d: %s", successCount, minISR, strings.Join(reasons, "; "))
	}

	return nil
}

func (cc *ClusterController) ReplicateToFollowers(topic string, partition int, msgCmd types.MessageCommand, minISR int) error {
	replicationStart := time.Now()

	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return fmt.Errorf("FSM not available")
	}

	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return fmt.Errorf("partition metadata not found: %s", partitionKey)
	}
	if meta.Leader != cc.brokerID {
		return fmt.Errorf("partition leader fenced: current=%s local=%s epoch=%d", meta.Leader, cc.brokerID, meta.LeaderEpoch)
	}
	if len(meta.ISR) < minISR {
		return fmt.Errorf("insufficient in-sync replicas: got %d, want minISR %d", len(meta.ISR), minISR)
	}
	isr := make(map[string]struct{}, len(meta.ISR))
	for _, brokerID := range meta.ISR {
		isr[brokerID] = struct{}{}
	}
	if _, ok := isr[cc.brokerID]; !ok {
		return fmt.Errorf("partition leader %s is not in ISR", cc.brokerID)
	}
	msgCmd.LeaderID = meta.Leader
	msgCmd.LeaderEpoch = meta.LeaderEpoch

	// Fan out to all replicas for catch-up, but only current ISR acknowledgements commit data.
	targets := []string{}
	for _, replica := range meta.Replicas {
		if replica != cc.brokerID {
			targets = append(targets, replica)
		}
	}

	data, err := json.Marshal(msgCmd)
	if err != nil {
		return err
	}
	replicateCmd := fmt.Sprintf("REPLICATE_MESSAGE %spayload=%s", cc.internalAuthPrefix(), string(data))

	var wg sync.WaitGroup
	var successCount int32 = 1 // The local leader is durable before replication begins.
	var mu sync.Mutex
	errCh := make(chan error, len(targets))

	partitionStr := fmt.Sprintf("%d", partition)
	for _, targetID := range targets {
		broker := fsm.GetBroker(targetID)
		if broker == nil {
			if _, required := isr[targetID]; required {
				errCh <- fmt.Errorf("ISR broker %s metadata not found", targetID)
			}
			continue
		}

		wg.Add(1)
		go func(addr, brokerID string) {
			defer wg.Done()
			resp, err := cc.Router.forwardWithTimeout(addr, replicateCmd)
			if err == nil && strings.HasPrefix(resp, "OK") {
				if _, required := isr[brokerID]; required {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
				metrics.ClusterReplicationLag.WithLabelValues(topic, partitionStr, brokerID).Observe(time.Since(replicationStart).Seconds())
				if isrMgr := cc.RaftManager.GetISRManager(); isrMgr != nil {
					isrMgr.UpdateHeartbeat(brokerID)
				}
				return
			}
			if err != nil {
				errCh <- err
			} else {
				errCh <- fmt.Errorf("replica %s rejected append: %s", brokerID, resp)
			}
		}(broker.Addr, targetID)
	}

	wg.Wait()
	close(errCh)

	if int(successCount) < len(meta.ISR) {
		reasons := make([]string, 0, len(errCh))
		for err := range errCh {
			reasons = append(reasons, err.Error())
		}
		return fmt.Errorf("not all ISR replicas acknowledged: got %d, want %d: %s", successCount, len(meta.ISR), strings.Join(reasons, "; "))
	}

	return nil
}
