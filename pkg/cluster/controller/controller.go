package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/metrics"
	topicpkg "github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

var ErrPartitionLeaderFenced = errors.New("partition leader fenced")

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

// PartitionReplicationSnapshot is an immutable view of the replication fence
// and replica sets used by one partition append.
type PartitionReplicationSnapshot struct {
	Leader         string
	LeaderEpoch    int
	LifecycleEpoch uint64
	ISR            []string
	Replicas       []string
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
	snapshot, err := cc.GetPartitionReplicationSnapshot(topic, partition)
	if err != nil {
		return err
	}
	if len(snapshot.ISR) < minISR {
		return fmt.Errorf("insufficient in-sync replicas: got %d, want minISR %d", len(snapshot.ISR), minISR)
	}
	if err := cc.ReplicateToISR(topic, partition, msgCmd, snapshot); err != nil {
		return err
	}
	_ = cc.ReplicateToNonISR(topic, partition, msgCmd, snapshot)
	return nil
}

func (cc *ClusterController) GetPartitionReplicationSnapshot(topic string, partition int) (PartitionReplicationSnapshot, error) {
	if cc == nil || cc.RaftManager == nil {
		return PartitionReplicationSnapshot{}, fmt.Errorf("cluster metadata unavailable")
	}
	fsmRef := cc.RaftManager.GetFSM()
	if fsmRef == nil {
		return PartitionReplicationSnapshot{}, fmt.Errorf("FSM not available")
	}
	partitionKey := topic + "-" + strconv.Itoa(partition)
	meta := fsmRef.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return PartitionReplicationSnapshot{}, fmt.Errorf("partition metadata not found: %s", partitionKey)
	}
	if meta.Leader != cc.brokerID {
		return PartitionReplicationSnapshot{}, fmt.Errorf("%w: current=%s local=%s epoch=%d", ErrPartitionLeaderFenced, meta.Leader, cc.brokerID, meta.LeaderEpoch)
	}
	if !containsBroker(meta.ISR, cc.brokerID) {
		return PartitionReplicationSnapshot{}, fmt.Errorf("partition leader %s is not in ISR", cc.brokerID)
	}
	lifecycleEpoch := meta.LifecycleEpoch
	if lifecycleEpoch == 0 {
		lifecycleEpoch = topicpkg.InitialLifecycleEpoch
	}
	return PartitionReplicationSnapshot{
		Leader:         meta.Leader,
		LeaderEpoch:    meta.LeaderEpoch,
		LifecycleEpoch: lifecycleEpoch,
		ISR:            append([]string(nil), meta.ISR...),
		Replicas:       append([]string(nil), meta.Replicas...),
	}, nil
}

// ReplicateToISR sends an append only to the ISR captured for this task. It is
// deliberately sequential: the partition replication lane already provides
// concurrency, so a publish never creates an unbounded set of goroutines.
func (cc *ClusterController) ReplicateToISR(topic string, partition int, msgCmd types.MessageCommand, snapshot PartitionReplicationSnapshot) error {
	return cc.replicateToReplicaSet(topic, partition, msgCmd, snapshot, snapshot.ISR, true)
}

// ReplicateToNonISR makes one best-effort catch-up pass after the committed HWM
// is visible. Failures here never change the producer acknowledgement.
func (cc *ClusterController) ReplicateToNonISR(topic string, partition int, msgCmd types.MessageCommand, snapshot PartitionReplicationSnapshot) error {
	isr := make(map[string]struct{}, len(snapshot.ISR))
	for _, brokerID := range snapshot.ISR {
		isr[brokerID] = struct{}{}
	}
	targets := make([]string, 0, len(snapshot.Replicas))
	for _, brokerID := range snapshot.Replicas {
		if _, required := isr[brokerID]; !required {
			targets = append(targets, brokerID)
		}
	}
	return cc.replicateToReplicaSet(topic, partition, msgCmd, snapshot, targets, false)
}

func (cc *ClusterController) replicateToReplicaSet(topic string, partition int, msgCmd types.MessageCommand, snapshot PartitionReplicationSnapshot, targets []string, required bool) error {
	current, err := cc.GetPartitionReplicationSnapshot(topic, partition)
	if err != nil {
		return err
	}
	if current.Leader != snapshot.Leader || current.LeaderEpoch != snapshot.LeaderEpoch {
		return fmt.Errorf("%w: current=%s/%d requested=%s/%d", ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, snapshot.Leader, snapshot.LeaderEpoch)
	}
	if current.LifecycleEpoch != snapshot.LifecycleEpoch {
		return fmt.Errorf("%w: current lifecycle=%d requested=%d", ErrPartitionLeaderFenced, current.LifecycleEpoch, snapshot.LifecycleEpoch)
	}
	if msgCmd.LifecycleEpoch == 0 {
		if current.LifecycleEpoch > topicpkg.InitialLifecycleEpoch {
			return fmt.Errorf("missing topic lifecycle epoch for %s-%d", topic, partition)
		}
		msgCmd.LifecycleEpoch = current.LifecycleEpoch
	} else if msgCmd.LifecycleEpoch != current.LifecycleEpoch {
		return fmt.Errorf("stale topic lifecycle epoch for %s-%d: current=%d requested=%d", topic, partition, current.LifecycleEpoch, msgCmd.LifecycleEpoch)
	}
	msgCmd.LeaderID = snapshot.Leader
	msgCmd.LeaderEpoch = snapshot.LeaderEpoch
	data, err := json.Marshal(msgCmd)
	if err != nil {
		return err
	}
	replicateCmd := fmt.Sprintf("REPLICATE_MESSAGE %spayload=%s", cc.internalAuthPrefix(), string(data))
	fsmRef := cc.RaftManager.GetFSM()
	partitionStr := strconv.Itoa(partition)
	var replicationErr error
	for _, brokerID := range targets {
		if brokerID == cc.brokerID {
			continue
		}
		broker := fsmRef.GetBroker(brokerID)
		if broker == nil {
			if required {
				return fmt.Errorf("ISR broker %s metadata not found", brokerID)
			}
			replicationErr = errors.Join(replicationErr, fmt.Errorf("replica %s metadata not found", brokerID))
			continue
		}
		started := time.Now()
		resp, forwardErr := cc.Router.forwardWithTimeout(broker.Addr, replicateCmd)
		if forwardErr != nil || !strings.HasPrefix(resp, "OK") {
			var targetErr error
			if forwardErr != nil {
				targetErr = fmt.Errorf("replica %s append failed: %w", brokerID, forwardErr)
			} else if replicaFenceResponse(resp) {
				targetErr = fmt.Errorf("%w: replica %s rejected append: %s", ErrPartitionLeaderFenced, brokerID, resp)
			} else {
				targetErr = fmt.Errorf("replica %s rejected append: %s", brokerID, resp)
			}
			if required {
				return targetErr
			}
			replicationErr = errors.Join(replicationErr, targetErr)
			continue
		}
		metrics.ClusterReplicationLag.WithLabelValues(topic, partitionStr, brokerID).Observe(time.Since(started).Seconds())
		if isrMgr := cc.RaftManager.GetISRManager(); isrMgr != nil {
			isrMgr.UpdateHeartbeat(brokerID)
		}
	}
	return replicationErr
}

func replicaFenceResponse(response string) bool {
	fields := strings.Fields(strings.TrimSpace(response))
	if len(fields) < 2 || !strings.EqualFold(fields[0], "ERROR:") {
		return false
	}
	code := strings.ToUpper(fields[1])
	return code == "NOT_PARTITION_LEADER" || code == "STALE_LEADER_EPOCH" ||
		code == "STALE_TOPIC_LIFECYCLE_EPOCH" || code == "MISSING_TOPIC_LIFECYCLE_EPOCH"
}

func containsBroker(brokers []string, wanted string) bool {
	for _, brokerID := range brokers {
		if brokerID == wanted {
			return true
		}
	}
	return false
}
