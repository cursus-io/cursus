package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type barrierReplicationExecutor struct {
	mu             sync.Mutex
	snapshot       clusterController.PartitionReplicationSnapshot
	started        chan struct{}
	barrier        chan struct{}
	replicateErr   error
	committedHWM   uint64
	commitHook     func()
	replicateCalls int
	nonISRCalls    int
	nonISRBarrier  chan struct{}
}

func (e *barrierReplicationExecutor) Snapshot(string, int) (clusterController.PartitionReplicationSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshot, nil
}

func (e *barrierReplicationExecutor) ReplicateISR(ctx context.Context, _ partitionReplicationTask, _ clusterController.PartitionReplicationSnapshot) error {
	e.mu.Lock()
	e.replicateCalls++
	started := e.started
	barrier := e.barrier
	err := e.replicateErr
	e.mu.Unlock()
	select {
	case started <- struct{}{}:
	default:
	}
	select {
	case <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *barrierReplicationExecutor) ReplicateNonISR(partitionReplicationTask, clusterController.PartitionReplicationSnapshot) error {
	e.mu.Lock()
	e.nonISRCalls++
	barrier := e.nonISRBarrier
	e.mu.Unlock()
	if barrier != nil {
		<-barrier
	}
	return nil
}

func (e *barrierReplicationExecutor) Commit(task partitionReplicationTask) error {
	e.mu.Lock()
	e.committedHWM = task.commitHWM
	hook := e.commitHook
	e.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (e *barrierReplicationExecutor) committed() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.committedHWM
}

func newBarrierReplicationExecutor() *barrierReplicationExecutor {
	return &barrierReplicationExecutor{
		snapshot: clusterController.PartitionReplicationSnapshot{
			Leader:         "broker-1",
			LeaderEpoch:    7,
			LifecycleEpoch: topic.InitialLifecycleEpoch,
			ISR:            []string{"broker-1", "broker-2"},
			Replicas:       []string{"broker-1", "broker-2", "broker-3"},
		},
		started: make(chan struct{}, 1),
		barrier: make(chan struct{}),
	}
}

func replicationTaskForMode(executor *barrierReplicationExecutor, mode ackpolicy.Mode) partitionReplicationTask {
	return partitionReplicationTask{
		topic:     "orders",
		partition: 0,
		commitHWM: 1,
		ackMode:   mode,
		snapshot:  executor.snapshot,
		result:    make(chan error, 1),
	}
}

func TestAllAcknowledgementWaitsForISRAndCommit(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(2, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	<-executor.started
	select {
	case err := <-task.result:
		t.Fatalf("all acknowledgement completed before follower barrier: %v", err)
	default:
	}
	require.Zero(t, executor.committed(), "HWM advanced before follower acknowledgement")

	close(executor.barrier)
	require.NoError(t, <-task.result)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
}

func TestLeaderAcknowledgementQueuesReplicationWithoutWaiting(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(2, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.Leader)
	task.result = nil

	started := time.Now()
	reservation.submit(task)
	require.Less(t, time.Since(started), 50*time.Millisecond)
	<-executor.started
	require.Zero(t, executor.committed())

	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
}

func TestIdempotentDuplicateBarrierDoesNotReplicateOrCommit(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	task.barrierOnly = true
	reservation.submit(task)

	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Zero(t, executor.replicateCalls)
	executor.mu.Unlock()
	require.Zero(t, executor.committed())
}

func TestReplicationQueueAppliesBoundedBackpressure(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	first, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.Leader)
	task.result = nil
	first.submit(task)
	<-executor.started

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = coordinator.reserve(ctx, "orders", 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(executor.barrier)
}

func TestReplicationQueueShutdownCancelsBlockedWorkerAndWaiter(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started

	done := make(chan struct{})
	go func() {
		coordinator.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replication coordinator leaked a blocked worker during shutdown")
	}
	require.ErrorIs(t, <-task.result, errReplicationQueueClosed)
	_, err = coordinator.reserve(context.Background(), "orders", 0)
	require.True(t, errors.Is(err, errReplicationQueueClosed))
}

func TestReplicationQueueShutdownUnblocksBackpressuredReservation(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	first, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.Leader)
	task.result = nil
	first.submit(task)
	<-executor.started

	reserveErr := make(chan error, 1)
	go func() {
		_, err := coordinator.reserve(context.Background(), "orders", 0)
		reserveErr <- err
	}()
	closeDone := make(chan struct{})
	go func() {
		coordinator.close()
		close(closeDone)
	}()
	require.ErrorIs(t, <-reserveErr, errReplicationQueueClosed)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown deadlocked behind a backpressured reservation")
	}
}

func TestReplicationQueueDoesNotCommitAfterLeaderEpochChanges(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started

	executor.mu.Lock()
	executor.snapshot.LeaderEpoch++
	executor.mu.Unlock()
	close(executor.barrier)

	require.ErrorContains(t, <-task.result, "fenced")
	require.Zero(t, executor.committed())
}

func TestAllAcknowledgementFailsWhenLeaderEpochChangesAfterCommit(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.commitHook = func() {
		executor.mu.Lock()
		executor.snapshot.LeaderEpoch++
		executor.mu.Unlock()
	}
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started
	close(executor.barrier)

	require.ErrorContains(t, <-task.result, "fenced after commit")
	require.Equal(t, uint64(1), executor.committed(), "commit may be durable even when the producer receives a fenced result")
}

func TestAllAcknowledgementDoesNotWaitForNonISRReplica(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.nonISRBarrier = make(chan struct{})
	coordinator := newPartitionReplicationCoordinator(1, executor)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started
	close(executor.barrier)

	require.NoError(t, <-task.result)
	require.Equal(t, uint64(1), executor.committed())
	close(executor.nonISRBarrier)
	coordinator.close()
}

func TestBlockedNonISRReplicaDoesNotDelayNextPartitionTask(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.nonISRBarrier = make(chan struct{})
	coordinator := newPartitionReplicationCoordinator(1, executor)
	firstReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	first := replicationTaskForMode(executor, ackpolicy.All)
	firstReservation.submit(first)
	<-executor.started
	close(executor.barrier)
	require.NoError(t, <-first.result)
	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.nonISRCalls == 1
	}, time.Second, time.Millisecond)

	secondReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	second := replicationTaskForMode(executor, ackpolicy.All)
	second.commitHWM = 2
	secondReservation.submit(second)
	select {
	case err := <-second.result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("non-ISR catch-up blocked the next partition task")
	}

	close(executor.nonISRBarrier)
	coordinator.close()
}

func TestCommittedNonISRTasksAreNotDroppedWhenCatchupQueueIsFull(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.nonISRBarrier = make(chan struct{})
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)

	for commitHWM := uint64(1); commitHWM <= 3; commitHWM++ {
		reservation, err := coordinator.reserve(context.Background(), "orders", 0)
		require.NoError(t, err)
		task := replicationTaskForMode(executor, ackpolicy.All)
		task.commitHWM = commitHWM
		reservation.submit(task)
		require.NoError(t, <-task.result)
		if commitHWM == 1 {
			require.Eventually(t, func() bool {
				executor.mu.Lock()
				defer executor.mu.Unlock()
				return executor.nonISRCalls == 1
			}, time.Second, time.Millisecond)
		}
	}

	close(executor.nonISRBarrier)
	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.nonISRCalls == 3
	}, time.Second, time.Millisecond)
	coordinator.close()
}

func TestDistributedLeaderAcknowledgementReturnsBeforeFollowerAndKeepsReplicating(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)

	started := time.Now()
	response := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	require.Contains(t, response, `"status":"OK"`)
	require.Less(t, time.Since(started), 100*time.Millisecond)
	<-executor.started
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Zero(t, partition.GetHWM(), "leader-only append became consumer-visible")
	require.Zero(t, executor.committed())

	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
}

func TestDistributedLeaderAcknowledgementDoesNotRequireEffectiveMinimumISR(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1"})
	executor.mu.Lock()
	executor.snapshot.ISR = []string{"broker-1"}
	executor.mu.Unlock()

	response := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	require.Contains(t, response, `"status":"OK"`)
	<-executor.started
	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
}

func TestDistributedPublishMigratesLegacyUnknownHWMBeforeAppend(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	legacyMetadata := `{"leader":"broker-1","leader_epoch":7,"replicas":["broker-1","broker-2"],"isr":["broker-1","broker-2"],"partition_count":1}`
	result := handler.Cluster.RaftManager.GetFSM().Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + legacyMetadata)})
	require.Nil(t, result)
	require.False(t, handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata("orders-0").CommittedHWMKnown)

	response := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	require.Contains(t, response, `"status":"OK"`)
	metadata := handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata("orders-0")
	require.True(t, metadata.CommittedHWMKnown)
	require.Zero(t, metadata.CommittedHWM, "legacy boundary was migrated after the new append")

	<-executor.started
	close(executor.barrier)
}

func TestDistributedIdempotentDuplicateAllUsesFenceBarrierOnly(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	close(executor.barrier)
	firstCommand := "PUBLISH topic=orders partition=0 acks=all producerId=p1 isIdempotent=true seqNum=1 epoch=7 message=value"

	first := handler.HandleCommand(firstCommand, NewClientContext("", 0))
	require.Contains(t, first, `"status":"OK"`)
	second := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 isIdempotent=true seqNum=2 epoch=7 message=later", NewClientContext("", 0))
	require.Contains(t, second, `"status":"OK"`)
	duplicate := handler.HandleCommand(firstCommand, NewClientContext("", 0))
	require.Contains(t, duplicate, `"status":"OK"`)
	require.Contains(t, duplicate, `"last_offset":0`)
	executor.mu.Lock()
	require.Equal(t, 2, executor.replicateCalls)
	executor.mu.Unlock()
	require.Equal(t, uint64(2), executor.committed())
}

func TestDistributedIdempotentDuplicateBatchReturnsOriginalOffset(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	close(executor.barrier)
	publish := func(seq uint64, payload string) string {
		data, err := util.EncodeBatchMessages("orders", 0, "all", true, []types.Message{{
			ProducerID: "p1",
			Epoch:      7,
			SeqNum:     seq,
			Payload:    payload,
		}})
		require.NoError(t, err)
		response, err := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
		require.NoError(t, err)
		return response
	}

	require.Contains(t, publish(1, "value"), `"last_offset":0`)
	require.Contains(t, publish(2, "later"), `"last_offset":1`)
	require.Contains(t, publish(1, "value"), `"last_offset":0`)
	executor.mu.Lock()
	require.Equal(t, 2, executor.replicateCalls)
	executor.mu.Unlock()
}

func TestReplicaPreservesLegacyTailUntilCommittedHWMIsKnown(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(manager, nil)
	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(manager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		for _, name := range manager.ListTopics() {
			for _, partition := range manager.GetTopic(name).Partitions {
				partition.Close()
			}
		}
		diskManager.CloseAllHandlers()
	})

	legacyMetadata := `{"leader":"broker-1","leader_epoch":7,"replicas":["broker-1","broker-2"],"isr":["broker-1","broker-2"],"partition_count":1}`
	require.Nil(t, state.Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + legacyMetadata)}))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	legacyTail := []types.Message{{Payload: "legacy"}}
	require.NoError(t, partition.EnqueueBatchLeader(legacyTail))
	require.Equal(t, uint64(1), partition.NextOffset())

	replication := types.MessageCommand{
		Topic: "orders", Partition: 0, LeaderID: "broker-1", LeaderEpoch: 7,
		Messages: []types.Message{{Offset: 1, Payload: "new"}},
	}
	payload, err := json.Marshal(replication)
	require.NoError(t, err)
	response := handler.handleReplicateMessage("REPLICATE_MESSAGE payload=" + string(payload))
	require.Contains(t, response, "committed HWM is not known")
	require.Equal(t, uint64(1), partition.NextOffset())
	messages, err := partition.ReadMessages(0, 2)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "legacy", messages[0].Payload)
}

func TestReplicaNewLeaderEpochReconcilesUncommittedOldLeaderTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(manager, nil)
	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(manager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		diskManager.CloseAllHandlers()
	})

	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
		PartitionCount: 1,
	})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	oldTail := []types.Message{{Payload: "old-uncommitted"}}
	require.NoError(t, partition.EnqueueBatchLeader(oldTail))
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Zero(t, partition.GetHWM())

	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-3", LeaderEpoch: 8,
		Replicas: []string{"broker-2", "broker-3"}, ISR: []string{"broker-2", "broker-3"},
		PartitionCount: 1,
	})
	replacement := types.MessageCommand{
		Topic: "orders", Partition: 0, LeaderID: "broker-3", LeaderEpoch: 8,
		Messages: []types.Message{{Offset: 0, Payload: "replacement"}},
	}
	payload, err := json.Marshal(replacement)
	require.NoError(t, err)
	response := handler.handleReplicateMessage("REPLICATE_MESSAGE payload=" + string(payload))
	require.Contains(t, response, "OK")
	require.NoError(t, partition.ApplyReplicaHWM(1))
	messages, err := partition.ReadMessages(0, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "replacement", messages[0].Payload)
}

func TestAllInsufficientISRRejectsBeforeLeadershipReconciliation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.MinInSyncReplicas = 2
	diskManager := disk.NewDiskManager(cfg)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(manager, nil)
	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	raftManager.leaderAddress.Store("broker-2:9001")
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(manager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		diskManager.CloseAllHandlers()
	})

	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	oldTail := []types.Message{{Payload: "old-uncommitted"}}
	require.NoError(t, partition.EnqueueBatchLeader(oldTail))
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Zero(t, partition.GetHWM())
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-2", LeaderEpoch: 8,
		Replicas: []string{"broker-2", "broker-3"}, ISR: []string{"broker-2"},
		PartitionCount: 1,
	})

	response := handler.HandleCommand(
		"PUBLISH topic=orders partition=0 acks=all producerId=p1 message=rejected",
		NewClientContext("", 0),
	)
	require.Contains(t, response, "ERROR: insufficient_in_sync_replicas")
	require.Equal(t, uint64(1), partition.NextOffset(), "ISR rejection reconciled or appended local state")
	require.Zero(t, partition.GetHWM())
}

func TestDistributedLeaderAcknowledgementsPreserveOrderedUncommittedTail(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)

	first := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=one", NewClientContext("", 0))
	require.Contains(t, first, `"last_offset":0`)
	<-executor.started
	second := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=two", NewClientContext("", 0))
	require.Contains(t, second, `"last_offset":1`)
	require.Equal(t, uint64(2), partition.NextOffset(), "next publish truncated the prior uncommitted tail")
	require.Zero(t, partition.GetHWM())

	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 2 }, time.Second, time.Millisecond)
}

func TestDistributedAllAcknowledgementBlocksUntilFollower(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})

	response := make(chan string, 1)
	go func() {
		response <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=value", NewClientContext("", 0))
	}()
	<-executor.started
	select {
	case got := <-response:
		t.Fatalf("acks=all returned before follower acknowledgement: %s", got)
	default:
	}
	close(executor.barrier)
	require.Contains(t, <-response, `"status":"OK"`)
}

func TestDistributedBatchUsesSameLeaderAndAllAcknowledgementPolicy(t *testing.T) {
	t.Run("leader", func(t *testing.T) {
		handler, manager, executor := newDistributedAckTestHandler(t, 2)
		require.NoError(t, manager.CreateTopic("orders", 1, false, false))
		installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
		partition, err := manager.GetTopic("orders").GetPartition(0)
		require.NoError(t, err)
		data, err := util.EncodeBatchMessages("orders", 0, "1", false, []types.Message{{Payload: "one", ProducerID: "p1"}, {Payload: "two", ProducerID: "p1"}})
		require.NoError(t, err)

		response, err := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
		require.NoError(t, err)
		require.Contains(t, response, `"status":"OK"`)
		<-executor.started
		require.Equal(t, uint64(2), partition.NextOffset())
		require.Zero(t, partition.GetHWM())
		close(executor.barrier)
		require.Eventually(t, func() bool { return executor.committed() == 2 }, time.Second, time.Millisecond)
	})

	t.Run("all", func(t *testing.T) {
		handler, manager, executor := newDistributedAckTestHandler(t, 2)
		require.NoError(t, manager.CreateTopic("orders", 1, false, false))
		installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
		data, err := util.EncodeBatchMessages("orders", 0, "all", false, []types.Message{{Payload: "one", ProducerID: "p1"}})
		require.NoError(t, err)
		response := make(chan string, 1)
		go func() {
			value, _ := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
			response <- value
		}()
		<-executor.started
		select {
		case got := <-response:
			t.Fatalf("batch acks=all returned before follower acknowledgement: %s", got)
		default:
		}
		close(executor.barrier)
		require.Contains(t, <-response, `"status":"OK"`)
	})
}

func TestDistributedAllRequestCancellationDoesNotLeakOrAbandonReplication(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	requestCtx, cancel := context.WithCancel(context.Background())
	clientCtx := NewClientContext("", 0)
	clientCtx.SetRequestContext(requestCtx)
	response := make(chan string, 1)
	go func() {
		response <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=value", clientCtx)
	}()
	<-executor.started
	cancel()
	require.Equal(t, "ERROR: request_cancelled", <-response)
	require.Zero(t, executor.committed())

	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
}

func TestAllAcknowledgementReportsFollowerTimeoutAndShutsDownCleanly(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = context.DeadlineExceeded
	coordinator := newPartitionReplicationCoordinator(1, executor)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started
	close(executor.barrier)
	require.ErrorIs(t, <-task.result, context.DeadlineExceeded)
	done := make(chan struct{})
	go func() {
		coordinator.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replication retry leaked after follower timeout and shutdown")
	}
}

func TestTopicEffectiveMinimumISRIsAppliedIndependently(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	one := 1
	two := 2
	require.NoError(t, manager.CreateTopicWithPolicy("available", 1, false, false, topic.Policy{MinInSyncReplicas: &one}))
	require.NoError(t, manager.CreateTopicWithPolicy("strict", 1, false, false, topic.Policy{MinInSyncReplicas: &two}))
	installPartitionMetadata(t, handler, "available", []string{"broker-1"})
	installPartitionMetadata(t, handler, "strict", []string{"broker-1"})
	close(executor.barrier)

	available := handler.HandleCommand("PUBLISH topic=available partition=0 acks=-1 producerId=p1 message=value", NewClientContext("", 0))
	require.Contains(t, available, `"status":"OK"`)
	strictPartition, err := manager.GetTopic("strict").GetPartition(0)
	require.NoError(t, err)
	before := strictPartition.NextOffset()
	strict := handler.HandleCommand("PUBLISH topic=strict partition=0 acks=all producerId=p2 message=value", NewClientContext("", 0))
	require.Contains(t, strict, "ERROR: insufficient_in_sync_replicas")
	require.Equal(t, before, strictPartition.NextOffset(), "insufficient ISR changed partition state")
}

func newDistributedAckTestHandler(t *testing.T, brokerMinISR int) (*CommandHandler, *topic.TopicManager, *barrierReplicationExecutor) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.MinInSyncReplicas = brokerMinISR
	cfg.ChannelBufferSize = 2
	diskManager := disk.NewDiskManager(cfg)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	state := fsm.NewBrokerFSM(manager, nil)
	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	raftManager.leaderAddress.Store("broker-1:9001")
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-1", "broker-1:9001")
	handler := NewCommandHandler(manager, cfg, nil, nil, cluster)
	handler.replication.close()
	executor := newBarrierReplicationExecutor()
	handler.replication = newPartitionReplicationCoordinator(2, executor)
	t.Cleanup(func() {
		_ = handler.Close()
		for _, name := range manager.ListTopics() {
			for _, partition := range manager.GetTopic(name).Partitions {
				partition.Close()
			}
		}
		diskManager.CloseAllHandlers()
	})
	return handler, manager, executor
}

func installPartitionMetadata(t *testing.T, handler *CommandHandler, topicName string, isr []string) {
	t.Helper()
	metadata := fmt.Sprintf(`{"leader":"broker-1","leader_epoch":7,"committed_hwm":0,"replicas":["broker-1","broker-2"],"isr":["%s"],"partition_count":1}`, strings.Join(isr, `","`))
	result := handler.Cluster.RaftManager.GetFSM().Apply(&raft.Log{Data: []byte("PARTITION:" + topicName + "-0:" + metadata)})
	require.Nil(t, result)
}

func applyPartitionMetadata(t *testing.T, state *fsm.BrokerFSM, topicName string, partition int, metadata fsm.PartitionMetadata) {
	t.Helper()
	metadata.CommittedHWMKnown = true
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	result := state.Apply(&raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s-%d:%s", topicName, partition, encoded))})
	require.Nil(t, result)
}
