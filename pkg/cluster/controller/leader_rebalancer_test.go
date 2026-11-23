package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func leaderRebalancePlannerSnapshot(topicName string, partitions int) fsm.LeaderRebalanceSnapshot {
	definition := topic.DefaultDefinition(topicName, nil)
	definition.Partitions = partitions
	definition.ReplicationFactor = 3
	snapshot := fsm.LeaderRebalanceSnapshot{
		Topics:     map[string]topic.Definition{topicName: definition},
		Partitions: make(map[string]fsm.PartitionMetadata, partitions),
		Brokers: map[string]fsm.BrokerInfo{
			"n1": {ID: "n1", Status: "active"},
			"n2": {ID: "n2", Status: "active"},
			"n3": {ID: "n3", Status: "active"},
		},
	}
	for partition := range partitions {
		key := fmt.Sprintf("%s-%d", topicName, partition)
		snapshot.Partitions[key] = fsm.PartitionMetadata{
			Leader: "n2", LeaderEpoch: partition + 1, LifecycleEpoch: definition.LifecycleEpoch,
			PartitionCount: partitions, Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2", "n3"},
		}
	}
	return snapshot
}

func mergeLeaderRebalanceSnapshots(target *fsm.LeaderRebalanceSnapshot, source fsm.LeaderRebalanceSnapshot) {
	for name, definition := range source.Topics {
		target.Topics[name] = definition
	}
	for key, metadata := range source.Partitions {
		target.Partitions[key] = metadata
	}
	for id, broker := range source.Brokers {
		target.Brokers[id] = broker
	}
}

func TestPlanPreferredLeaderRebalanceClassifiesExclusions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fsm.LeaderRebalanceSnapshot)
		want   LeaderRebalanceReason
	}{
		{name: "eligible", want: LeaderRebalanceEligible},
		{name: "already preferred", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.Leader = "n1"
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalanceAlreadyPreferred},
		{name: "assignment in progress", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.Replicas = metadata.Replicas[:2]
			metadata.ISR = metadata.ISR[:2]
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalanceAssignmentInProgress},
		{name: "recovery pending", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.RecoveryReplicas = []string{"n3"}
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalanceRecoveryPending},
		{name: "preferred inactive", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			broker := snapshot.Brokers["n1"]
			broker.Status = "inactive"
			snapshot.Brokers["n1"] = broker
		}, want: LeaderRebalancePreferredInactive},
		{name: "preferred outside isr", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.ISR = []string{"n2", "n3"}
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalancePreferredOutOfISR},
		{name: "stale lifecycle", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.LifecycleEpoch++
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalanceInvalidMetadata},
		{name: "duplicate replica", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			metadata := snapshot.Partitions["orders-0"]
			metadata.Replicas = []string{"n1", "n2", "n2"}
			snapshot.Partitions["orders-0"] = metadata
		}, want: LeaderRebalanceInvalidMetadata},
		{name: "unknown replica", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			delete(snapshot.Brokers, "n3")
		}, want: LeaderRebalanceInvalidMetadata},
		{name: "missing metadata", mutate: func(snapshot *fsm.LeaderRebalanceSnapshot) {
			delete(snapshot.Partitions, "orders-0")
		}, want: LeaderRebalanceMissingReplica},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := leaderRebalancePlannerSnapshot("orders", 1)
			if test.mutate != nil {
				test.mutate(&snapshot)
			}
			plan := PlanPreferredLeaderRebalance(snapshot, 8)
			require.Len(t, plan.Partitions, 1)
			require.Equal(t, test.want, plan.Partitions[0].Reason)
			require.Equal(t, test.want == LeaderRebalanceEligible, plan.Partitions[0].Eligible)
		})
	}
}

func TestPlanPreferredLeaderRebalanceReservesOneInternalMove(t *testing.T) {
	snapshot := fsm.LeaderRebalanceSnapshot{
		Topics: make(map[string]topic.Definition), Partitions: make(map[string]fsm.PartitionMetadata), Brokers: make(map[string]fsm.BrokerInfo),
	}
	mergeLeaderRebalanceSnapshots(&snapshot, leaderRebalancePlannerSnapshot(config.ConsumerOffsetsTopicName, 2))
	mergeLeaderRebalanceSnapshots(&snapshot, leaderRebalancePlannerSnapshot("orders", 10))

	plan := PlanPreferredLeaderRebalance(snapshot, 8)
	require.Equal(t, 12, plan.ImbalancedPartitions)
	require.Equal(t, 12, plan.EligiblePartitions)
	require.Len(t, plan.Candidates, 8)
	require.Equal(t, config.ConsumerOffsetsTopicName, plan.Candidates[0].Topic)
	require.Equal(t, 0, plan.Candidates[0].Partition)
	for index, candidate := range plan.Candidates[1:] {
		require.Equal(t, "orders", candidate.Topic)
		require.Equal(t, index, candidate.Partition)
	}
	require.Equal(t, map[string]int{"n2": 12}, plan.LeaderCounts)
	require.Equal(t, map[string]int{"n1": 12}, plan.PreferredLeaderCounts)
}

func TestPlanPreferredLeaderRebalanceUsesFullCapacityWithoutInternalCandidates(t *testing.T) {
	plan := PlanPreferredLeaderRebalance(leaderRebalancePlannerSnapshot("orders", 10), 8)
	require.Len(t, plan.Candidates, 8)
	for index, candidate := range plan.Candidates {
		require.Equal(t, index, candidate.Partition)
	}
}

type leaderRebalanceRaftManager struct {
	*MockRaftManager
	mu            sync.Mutex
	commands      []fsm.LeaderElectionCommand
	failPartition map[int]error
	loseAfter     int
	applied       chan struct{}
	beforeApply   func(*fsm.BrokerFSM, fsm.LeaderElectionCommand)
}

func (manager *leaderRebalanceRaftManager) ApplyCommand(prefix string, data []byte) error {
	if prefix != "LEADER_ELECTION" {
		return fmt.Errorf("unexpected command %s", prefix)
	}
	var command fsm.LeaderElectionCommand
	if err := json.Unmarshal(data, &command); err != nil {
		return err
	}
	manager.mu.Lock()
	manager.commands = append(manager.commands, command)
	count := len(manager.commands)
	err := manager.failPartition[command.Partition]
	if manager.loseAfter > 0 && count >= manager.loseAfter {
		manager.isLeader = false
	}
	beforeApply := manager.beforeApply
	manager.mu.Unlock()
	if err != nil {
		return err
	}
	if beforeApply != nil {
		beforeApply(manager.mockFSM, command)
	}
	result := manager.mockFSM.Apply(&raft.Log{Data: append([]byte(prefix+":"), data...)})
	if applyErr, ok := result.(error); ok {
		return applyErr
	}
	if manager.applied != nil {
		select {
		case manager.applied <- struct{}{}:
		default:
		}
	}
	return nil
}

func leaderRebalanceRunnerState(t *testing.T, partitions int) *fsm.BrokerFSM {
	t.Helper()
	state := fsm.NewBrokerFSM(nil, nil)
	for _, id := range []string{"n1", "n2", "n3"} {
		payload, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: id + ":9001", Status: "active"})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...)}))
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = partitions
	definition.ReplicationFactor = 3
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...)}))
	for partition := range partitions {
		key := fmt.Sprintf("orders-%d", partition)
		current := state.GetPartitionMetadata(key)
		leaderEpoch := current.LeaderEpoch
		if current.Leader != "n2" {
			leaderEpoch++
		}
		metadata := fsm.PartitionMetadata{
			Leader: "n2", LeaderEpoch: leaderEpoch, LifecycleEpoch: definition.LifecycleEpoch,
			PartitionCount: partitions, Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2", "n3"},
		}
		payload, err = json.Marshal(metadata)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: []byte("PARTITION:" + key + ":" + string(payload))}))
	}
	return state
}

func TestRunLeaderRebalanceOnceContinuesAfterPartitionFailure(t *testing.T) {
	state := leaderRebalanceRunnerState(t, 3)
	manager := &leaderRebalanceRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
		failPartition:   map[int]error{0: errors.New("stale plan")},
	}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LeaderRebalanceMaxMovesPerCycle = 2
	controller := &ClusterController{RaftManager: manager, Config: cfg}

	result := controller.RunLeaderRebalanceOnce(context.Background())
	require.Len(t, result.Moves, 2)
	require.Equal(t, "failure", result.Moves[0].Result)
	require.Equal(t, "success", result.Moves[1].Result)
	require.Equal(t, "n2", state.GetPartitionMetadata("orders-0").Leader)
	require.Equal(t, "n1", state.GetPartitionMetadata("orders-1").Leader)
	require.Equal(t, "n2", state.GetPartitionMetadata("orders-2").Leader)
	require.Len(t, manager.commands, 2)
	require.Equal(t, fsm.LeaderElectionTopologyFenceVersionCurrent, manager.commands[1].TopologyFenceVersion)
	require.Equal(t, uint64(topic.InitialDefinitionRevision), manager.commands[1].ExpectedDefinitionRevision)
}

func TestRunLeaderRebalanceOnceRejectsStateChangesAfterPlanning(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *fsm.BrokerFSM)
		want   string
	}{
		{
			name: "topic definition revision",
			mutate: func(t *testing.T, state *fsm.BrokerFSM) {
				minISR := 2
				payload, err := json.Marshal(fsm.TopicConfigCommand{Name: "orders", MinInSyncReplicas: &minISR})
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC_CONFIG:"), payload...)}))
			},
			want: "stale definition revision",
		},
		{
			name: "replica recovery",
			mutate: func(t *testing.T, state *fsm.BrokerFSM) {
				metadata := state.GetPartitionMetadata("orders-0")
				require.NotNil(t, metadata)
				metadata.RecoveryReplicas = []string{"n3"}
				payload, err := json.Marshal(metadata)
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), payload...)}))
			},
			want: "replica recovery pending",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := leaderRebalanceRunnerState(t, 1)
			manager := &leaderRebalanceRaftManager{
				MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
				beforeApply: func(state *fsm.BrokerFSM, _ fsm.LeaderElectionCommand) {
					test.mutate(t, state)
				},
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LeaderRebalanceMaxMovesPerCycle = 1

			result := (&ClusterController{RaftManager: manager, Config: cfg}).RunLeaderRebalanceOnce(context.Background())
			require.Len(t, result.Moves, 1)
			require.Equal(t, "failure", result.Moves[0].Result)
			require.ErrorContains(t, result.Moves[0].Err, test.want)
			require.Equal(t, "n2", state.GetPartitionMetadata("orders-0").Leader)
		})
	}
}

func TestRunLeaderRebalanceOnceStopsAfterLeadershipLoss(t *testing.T) {
	state := leaderRebalanceRunnerState(t, 3)
	manager := &leaderRebalanceRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state}, loseAfter: 1,
	}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	controller := &ClusterController{RaftManager: manager, Config: cfg}

	result := controller.RunLeaderRebalanceOnce(context.Background())
	require.Len(t, result.Moves, 1)
	require.Equal(t, "not_leader", result.Skipped)
}

func TestStartLeaderRebalancerWaitsForFirstIntervalAndStops(t *testing.T) {
	state := leaderRebalanceRunnerState(t, 1)
	manager := &leaderRebalanceRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state}, applied: make(chan struct{}, 1),
	}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	trigger := make(chan struct{})
	controller := &ClusterController{RaftManager: manager, Config: cfg}
	controller.leaderRebalanceWait = func(ctx context.Context, _ time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-trigger:
			return true
		}
	}
	stop := controller.StartLeaderRebalancer(context.Background())
	select {
	case <-manager.applied:
		t.Fatal("leader moved before the first interval")
	default:
	}
	trigger <- struct{}{}
	select {
	case <-manager.applied:
	case <-time.After(5 * time.Second):
		t.Fatal("leader rebalance did not run after interval")
	}
	stop()
	require.Equal(t, "n1", state.GetPartitionMetadata("orders-0").Leader)
}

func TestStartLeaderRebalancerDisabledIsNoop(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LeaderRebalanceEnabled = false
	controller := &ClusterController{Config: cfg}
	stop := controller.StartLeaderRebalancer(context.Background())
	stop()
}
