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
	mu                 sync.Mutex
	snapshot           clusterController.PartitionReplicationSnapshot
	started            chan struct{}
	barrier            chan struct{}
	replicateErr       error
	replicateFailures  int
	committedHWM       uint64
	commitHook         func()
	commitErr          error
	skipLocalCommit    bool
	replicateCalls     int
	replicateSnapshots []clusterController.PartitionReplicationSnapshot
	nonISRCalls        int
	nonISRBarrier      chan struct{}
	recoverGap         func(partitionReplicationTask, error) (replicaGapRecoveryResult, error)
	state              *fsm.BrokerFSM
}

func (e *barrierReplicationExecutor) RecoverReplicaGap(task partitionReplicationTask, cause error) (replicaGapRecoveryResult, error) {
	if e.recoverGap != nil {
		return e.recoverGap(task, cause)
	}
	return replicaGapRecoveryResult{}, cause
}

type permanentReplicationError struct{}

func (permanentReplicationError) Error() string { return "invalid replica append" }

func (permanentReplicationError) Retryable() bool { return false }

func (permanentReplicationError) ReplicationErrorClass() string { return "validation" }

type replicaGapTestError struct{ brokerID string }

func (e replicaGapTestError) Error() string                 { return "replica_offset_gap" }
func (e replicaGapTestError) Retryable() bool               { return true }
func (e replicaGapTestError) ReplicationErrorClass() string { return "availability" }
func (e replicaGapTestError) ReplicationErrorCode() string  { return "replica_offset_gap" }
func (e replicaGapTestError) ReplicaBrokerID() string       { return e.brokerID }

func (e *barrierReplicationExecutor) Snapshot(string, int) (clusterController.PartitionReplicationSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshot, nil
}

func (e *barrierReplicationExecutor) ReplicateISR(ctx context.Context, _ partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error {
	e.mu.Lock()
	e.replicateCalls++
	snapshot.ISR = append([]string(nil), snapshot.ISR...)
	snapshot.Replicas = append([]string(nil), snapshot.Replicas...)
	e.replicateSnapshots = append(e.replicateSnapshots, snapshot)
	started := e.started
	barrier := e.barrier
	err := error(nil)
	if e.replicateFailures != 0 {
		err = e.replicateErr
		if e.replicateFailures > 0 {
			e.replicateFailures--
		}
	}
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

func (e *barrierReplicationExecutor) Commit(task partitionReplicationTask) (partitionCommitResult, error) {
	e.mu.Lock()
	e.committedHWM = task.commitHWM
	hook := e.commitHook
	commitErr := e.commitErr
	skipLocalCommit := e.skipLocalCommit
	e.mu.Unlock()
	if task.partitionRef != nil && !skipLocalCommit {
		if err := task.partitionRef.ApplyReplicaHWM(task.commitHWM); err != nil {
			return partitionCommitResult{accepted: true, hwm: task.commitHWM}, err
		}
	}
	if hook != nil {
		hook()
	}
	if e.state != nil {
		metadata := e.state.GetPartitionMetadata(fmt.Sprintf("%s-%d", task.topic, task.partition))
		if metadata != nil {
			metadata.CommittedHWM = task.commitHWM
			metadata.CommittedHWMKnown = true
			encoded, err := json.Marshal(metadata)
			if err != nil {
				return partitionCommitResult{accepted: true, hwm: task.commitHWM}, err
			}
			if result := e.state.Apply(&raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s-%d:%s", task.topic, task.partition, encoded))}); result != nil {
				if applyErr, ok := result.(error); ok {
					return partitionCommitResult{accepted: true, hwm: task.commitHWM}, applyErr
				}
				return partitionCommitResult{accepted: true, hwm: task.commitHWM}, fmt.Errorf("unexpected partition metadata apply result: %v", result)
			}
		}
	}
	return partitionCommitResult{accepted: true, hwm: task.commitHWM}, commitErr
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
	requiredISR := 0
	if mode == ackpolicy.All {
		requiredISR = 2
	}
	return partitionReplicationTask{
		topic:       "orders",
		partition:   0,
		commitHWM:   1,
		ackMode:     mode,
		requiredISR: requiredISR,
		snapshot:    executor.snapshot,
		result:      make(chan error, 1),
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

func TestIdempotentDuplicateResumesReplicationBeforeAcknowledging(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.SetHWM(0)
	messages := []types.Message{{Payload: "value", ProducerID: "p1", Epoch: 7, SeqNum: 1}}
	require.NoError(t, partition.EnqueueBatchLeaderWithMode(messages, true))
	require.Zero(t, partition.GetHWM())

	reservation, err := handler.replication.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	task.duplicate = true
	task.partitionRef = partition
	task.command = types.MessageCommand{Topic: "orders", Partition: 0, Messages: []types.Message{{Offset: 0, Payload: "value", ProducerID: "p1", Epoch: 7, SeqNum: 1}}}
	reservation.submit(task)

	<-executor.started
	select {
	case err := <-task.result:
		t.Fatalf("duplicate acknowledgement crossed the HWM before commit: %v", err)
	default:
	}
	close(executor.barrier)
	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Equal(t, 1, executor.replicateCalls)
	executor.mu.Unlock()
	require.Equal(t, uint64(1), executor.committed())
	require.Equal(t, uint64(1), partition.GetHWM())
}

func TestLeaderReplicationFenceBlocksStaleCommittedHWMReconcile(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}}))
	require.NoError(t, partition.ApplyReplicaHWM(1))

	state := handler.Cluster.RaftManager.GetFSM()
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 1,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
		PartitionCount: 1,
	})

	releaseWrite, releaseMutation, _, err := handler.preparePartitionLeaderSnapshot("orders", 0, partition, 2)
	require.NoError(t, err)
	released := false
	t.Cleanup(func() {
		if !released {
			releaseMutation()
			releaseWrite()
		}
	})

	inFlight := []types.Message{{Payload: "lifecycle-snapshot"}}
	require.NoError(t, partition.EnqueueBatchLeader(inFlight))
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())

	reconcileResult := make(chan error, 1)
	go func() {
		reconcileResult <- partition.ReconcileCommittedHWM(1)
	}()

	select {
	case reconcileErr := <-reconcileResult:
		require.NoError(t, reconcileErr)
		applyErr := partition.ApplyReplicaHWM(2)
		require.ErrorContains(t, applyErr, "commit watermark 2 is ahead of local LEO 1")
		t.Fatal("stale reconcile truncated an in-flight leader append")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, partition.ApplyReplicaHWM(2))
	releaseMutation()
	releaseWrite()
	released = true
	select {
	case reconcileErr := <-reconcileResult:
		require.ErrorContains(t, reconcileErr, "committed HWM regression")
	case <-time.After(time.Second):
		t.Fatal("stale reconcile did not resume after replication mutation completed")
	}
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())
}

func TestAllAcknowledgementRetriesTransientFollowerFailureBeforeResponding(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = context.DeadlineExceeded
	executor.replicateFailures = 1
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.replicateCalls >= 1
	}, time.Second, time.Millisecond)
	select {
	case err := <-task.result:
		t.Fatalf("acks=all exposed a transient follower failure: %v", err)
	default:
	}
	require.NoError(t, <-task.result)
	require.Equal(t, uint64(1), executor.committed())
}

func TestReplicaOffsetGapQuarantinesOnceAndContinuesWithReducedISR(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = replicaGapTestError{brokerID: "broker-2"}
	executor.replicateFailures = 1
	executor.recoverGap = func(_ partitionReplicationTask, cause error) (replicaGapRecoveryResult, error) {
		require.True(t, replicaGapError(cause))
		executor.mu.Lock()
		executor.snapshot.ISR = []string{"broker-1", "broker-3"}
		executor.snapshot.RecoveryReplicas = []string{"broker-2"}
		executor.mu.Unlock()
		return replicaGapRecoveryResult{}, nil
	}
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Equal(t, 2, executor.replicateCalls)
	require.Equal(t, []string{"broker-1", "broker-2"}, executor.replicateSnapshots[0].ISR)
	require.Equal(t, []string{"broker-1", "broker-3"}, executor.replicateSnapshots[1].ISR)
	executor.mu.Unlock()
	require.Equal(t, uint64(1), executor.committed())
}

func TestLeaderAcknowledgedGapDecisionFailurePreservesTailAndRetries(t *testing.T) {
	_, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "acknowledged"}}))

	executor := newBarrierReplicationExecutor()
	executor.replicateErr = replicaGapTestError{brokerID: "broker-2"}
	executor.replicateFailures = 1
	secondRecoveryStarted := make(chan struct{})
	allowRecovery := make(chan struct{})
	recoveryCalls := 0
	executor.recoverGap = func(_ partitionReplicationTask, cause error) (replicaGapRecoveryResult, error) {
		require.True(t, replicaGapError(cause))
		recoveryCalls++
		if recoveryCalls == 1 {
			return replicaGapRecoveryResult{resolved: true}, errors.New("quarantine decision unavailable")
		}
		close(secondRecoveryStarted)
		<-allowRecovery
		executor.mu.Lock()
		executor.snapshot.ISR = []string{"broker-1", "broker-3"}
		executor.snapshot.RecoveryReplicas = []string{"broker-2"}
		executor.mu.Unlock()
		return replicaGapRecoveryResult{}, nil
	}
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	writeReleased := make(chan struct{})
	task := replicationTaskForMode(executor, ackpolicy.Leader)
	task.partitionRef = partition
	task.releaseMutation = partition.BeginReplicationMutation()
	task.releaseWrite = func() { close(writeReleased) }
	reservation.submit(task)

	select {
	case <-secondRecoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("leader-acknowledged gap recovery did not retry its decision")
	}
	require.Equal(t, uint64(1), partition.NextOffset(), "the acknowledged tail must not be rolled back")
	require.Zero(t, partition.GetHWM())
	require.ErrorContains(t, partition.RecoveryError(), "quarantine decision unavailable")
	executor.mu.Lock()
	require.Equal(t, 1, executor.replicateCalls, "replication must pause until the gap decision succeeds")
	executor.mu.Unlock()
	select {
	case <-writeReleased:
		t.Fatal("write ownership was released before the gap decision completed")
	default:
	}

	close(allowRecovery)
	require.NoError(t, <-task.result)
	require.Equal(t, uint64(1), partition.GetHWM())
	require.Equal(t, uint64(1), partition.NextOffset())
	require.NoError(t, partition.RecoveryError())
	select {
	case <-writeReleased:
	case <-time.After(time.Second):
		t.Fatal("write ownership was not released after commit")
	}
}

func TestRecoverReplicaGapDefersAuthoritativeReconcileUntilMutationRelease(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}}))
	applyPartitionMetadata(t, handler.Cluster.RaftManager.GetFSM(), "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 1,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
		PartitionCount: 1,
	})
	snapshot, err := handler.Cluster.GetPartitionReplicationSnapshot("orders", 0)
	require.NoError(t, err)
	releaseMutation := partition.BeginReplicationMutation()
	t.Cleanup(releaseMutation)

	type recoveryResponse struct {
		result replicaGapRecoveryResult
		err    error
	}
	done := make(chan recoveryResponse, 1)
	go func() {
		result, recoverErr := (clusterPartitionReplicationExecutor{handler: handler}).RecoverReplicaGap(
			partitionReplicationTask{
				topic: "orders", partition: 0, commitHWM: 1, ackMode: ackpolicy.All, requiredISR: 2,
				snapshot: snapshot, partitionRef: partition,
			},
			replicaGapTestError{brokerID: "broker-2"},
		)
		done <- recoveryResponse{result: result, err: recoverErr}
	}()
	select {
	case response := <-done:
		require.NoError(t, response.err)
		require.True(t, response.result.resolved)
		require.True(t, response.result.hasAuthoritativeHWM)
		require.Equal(t, uint64(1), response.result.authoritativeHWM)
	case <-time.After(time.Second):
		t.Fatal("replica gap recovery reconciled while holding the mutation read lock")
	}
}

func TestSameEpochPrepareTrimsUncommittedTailBeforeNextAppend(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	applyPartitionMetadata(t, handler.Cluster.RaftManager.GetFSM(), "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 1,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
		PartitionCount: 1,
	})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}}))
	require.NoError(t, partition.ApplyReplicaHWM(1))

	releaseWrite, releaseMutation, _, err := handler.preparePartitionLeaderSnapshot("orders", 0, partition, 2)
	require.NoError(t, err)
	releaseMutation()
	releaseWrite()
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 1, Payload: "uncommitted"}}))
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())

	releaseWrite, releaseMutation, _, err = handler.preparePartitionLeaderSnapshot("orders", 0, partition, 2)
	require.NoError(t, err)
	releaseMutation()
	releaseWrite()
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
}

func TestRecoveryPendingPartitionRejectsNewLeaderAppend(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	state := handler.Cluster.RaftManager.GetFSM()
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 0,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"},
		RecoveryReplicas: []string{"broker-2"}, PartitionCount: 1,
	})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	_, _, _, err = handler.preparePartitionLeaderSnapshot("orders", 0, partition, 1)
	require.ErrorContains(t, err, "replica_recovery_pending")
}

func TestLocalReconciliationPendingRejectsNewLeaderAppend(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	state := handler.Cluster.RaftManager.GetFSM()
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 0,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"},
		PartitionCount: 1,
	})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.MarkReconciliationPending(errors.New("quarantine decision unavailable"))

	_, _, _, err = handler.preparePartitionLeaderSnapshot("orders", 0, partition, 1)
	require.ErrorContains(t, err, "partition recovery pending")
	require.ErrorContains(t, err, "quarantine decision unavailable")
}

func TestAllAcknowledgementRefreshesISRWhileRetrying(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = context.DeadlineExceeded
	executor.replicateFailures = 1
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	<-executor.started
	executor.mu.Lock()
	executor.snapshot.ISR = []string{"broker-1", "broker-3"}
	executor.mu.Unlock()
	close(executor.barrier)

	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Len(t, executor.replicateSnapshots, 2)
	require.Equal(t, []string{"broker-1", "broker-2"}, executor.replicateSnapshots[0].ISR)
	require.Equal(t, []string{"broker-1", "broker-3"}, executor.replicateSnapshots[1].ISR)
	executor.mu.Unlock()
	require.Equal(t, uint64(1), executor.committed())
}

func TestAllAcknowledgementRechecksISRBeforeCommit(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	<-executor.started
	executor.mu.Lock()
	executor.snapshot.ISR = []string{"broker-1", "broker-3"}
	executor.mu.Unlock()
	close(executor.barrier)

	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Len(t, executor.replicateSnapshots, 2)
	require.Equal(t, []string{"broker-1", "broker-2"}, executor.replicateSnapshots[0].ISR)
	require.Equal(t, []string{"broker-1", "broker-3"}, executor.replicateSnapshots[1].ISR)
	executor.mu.Unlock()
	require.Equal(t, uint64(1), executor.committed())
}

func TestAllAcknowledgementWaitsForMinimumISRAfterMembershipChange(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	close(executor.barrier)
	executor.snapshot.ISR = []string{"broker-1"}
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)

	require.Never(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.replicateCalls != 0
	}, 50*time.Millisecond, 5*time.Millisecond)
	executor.mu.Lock()
	executor.snapshot.ISR = []string{"broker-1", "broker-3"}
	executor.mu.Unlock()

	require.NoError(t, <-task.result)
	executor.mu.Lock()
	require.Equal(t, 1, executor.replicateCalls)
	require.Equal(t, []string{"broker-1", "broker-3"}, executor.replicateSnapshots[0].ISR)
	executor.mu.Unlock()
}

func TestAllAcknowledgementReturnsPermanentFollowerFailureWithoutBlockingLane(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = permanentReplicationError{}
	executor.replicateFailures = 1
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)

	firstReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	first := replicationTaskForMode(executor, ackpolicy.All)
	firstReservation.submit(first)
	require.ErrorContains(t, <-first.result, "invalid replica append")

	secondReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	second := replicationTaskForMode(executor, ackpolicy.All)
	second.commitHWM = 2
	secondReservation.submit(second)
	require.NoError(t, <-second.result)

	executor.mu.Lock()
	require.Equal(t, 2, executor.replicateCalls, "permanent failure was retried or kept the lane occupied")
	executor.mu.Unlock()
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
	coordinator := newPartitionReplicationCoordinator(1, executor)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started
	close(executor.barrier)

	require.NoError(t, <-task.result)
	require.Equal(t, uint64(1), executor.committed())
	executor.mu.Lock()
	require.Zero(t, executor.nonISRCalls, "foreground replication must leave non-ISR catch-up to the bounded range worker")
	executor.mu.Unlock()
	coordinator.close()
}

func TestNonISRRangeCatchupCannotBlockForegroundPartitionLane(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	coordinator := newPartitionReplicationCoordinator(1, executor)
	firstReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	first := replicationTaskForMode(executor, ackpolicy.All)
	firstReservation.submit(first)
	<-executor.started
	close(executor.barrier)
	require.NoError(t, <-first.result)

	secondReservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	second := replicationTaskForMode(executor, ackpolicy.All)
	second.commitHWM = 2
	secondReservation.submit(second)
	select {
	case err := <-second.result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("non-ISR catch-up blocked the foreground partition task")
	}
	executor.mu.Lock()
	require.Zero(t, executor.nonISRCalls)
	executor.mu.Unlock()
	coordinator.close()
}

func TestForegroundCommitsDoNotQueueUnboundedPerMessageCatchup(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)

	for commitHWM := uint64(1); commitHWM <= 3; commitHWM++ {
		reservation, err := coordinator.reserve(context.Background(), "orders", 0)
		require.NoError(t, err)
		task := replicationTaskForMode(executor, ackpolicy.All)
		task.commitHWM = commitHWM
		reservation.submit(task)
		require.NoError(t, <-task.result)
	}

	executor.mu.Lock()
	require.Zero(t, executor.nonISRCalls)
	executor.mu.Unlock()
	coordinator.close()
}

func TestDistributedLeaderAcknowledgementReturnsBeforeFollowerAndKeepsReplicating(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)

	responseCh := make(chan string, 1)
	go func() {
		responseCh <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	}()
	select {
	case response := <-responseCh:
		require.Contains(t, response, `"status":"OK"`)
	case <-time.After(time.Second):
		t.Fatal("leader acknowledgement waited for follower replication")
	}
	<-executor.started
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Zero(t, partition.GetHWM(), "leader-only append became consumer-visible")
	require.Zero(t, executor.committed())
	reconcileResult := make(chan error, 1)
	go func() {
		reconcileResult <- partition.ReconcileCommittedHWM(0)
	}()
	select {
	case reconcileErr := <-reconcileResult:
		t.Fatalf("stale reconcile bypassed asynchronous replication fence: %v", reconcileErr)
	case <-time.After(100 * time.Millisecond):
	}

	close(executor.barrier)
	require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
	select {
	case reconcileErr := <-reconcileResult:
		require.ErrorContains(t, reconcileErr, "committed HWM regression")
	case <-time.After(time.Second):
		t.Fatal("stale reconcile did not resume after asynchronous replication completed")
	}
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

func TestDistributedPublishRejectsMarkerlessHWMMetadata(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	markerlessMetadata := `{"leader":"broker-1","leader_epoch":7,"lifecycle_epoch":1,"replicas":["broker-1","broker-2"],"isr":["broker-1","broker-2"],"partition_count":1}`
	result := handler.Cluster.RaftManager.GetFSM().Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + markerlessMetadata)})
	require.ErrorIs(t, result.(error), fsm.ErrUnsupportedRecoveryProtocol)

	response := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	require.Contains(t, response, "ERROR:")
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.Zero(t, partition.NextOffset())
	close(executor.barrier)
}

func TestDistributedPublishReleasesPartitionOwnershipWhenReplicationCoordinatorIsUnavailable(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	handler.replication.close()
	handler.replication = nil

	response := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", NewClientContext("", 0))
	require.Equal(t, "ERROR: cluster_metadata_unavailable command=PUBLISH", response)
	requirePartitionOwnershipAvailable(t, handler, partition)

	data, err := util.EncodeBatchMessages("orders", 0, "1", false, []types.Message{{Payload: "value", ProducerID: "p1"}})
	require.NoError(t, err)
	response, err = handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
	require.NoError(t, err)
	require.Equal(t, "ERROR: cluster_metadata_unavailable command=BATCH", response)
	requirePartitionOwnershipAvailable(t, handler, partition)
	require.Zero(t, partition.NextOffset())
}

func TestCompleteReplicationTaskReportsReconcileFailureAndDropsBlockedResult(t *testing.T) {
	_, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReconcileSnapshotHWM(0))

	mutationReleased := false
	writeReleased := false
	result := make(chan error, 1)
	completeReplicationTask(partitionReplicationTask{
		partitionRef:    partition,
		releaseMutation: func() { mutationReleased = true },
		releaseWrite:    func() { writeReleased = true },
		result:          result,
	}, errors.New("replication failed"))
	require.ErrorContains(t, <-result, "reconcile failed replication to committed HWM 0")
	require.True(t, mutationReleased)
	require.True(t, writeReleased)
	require.ErrorContains(t, partition.ReconciliationError(), "snapshot replay is still pending")
	require.ErrorContains(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "blocked"}}), "partition recovery incomplete")

	blockedResult := make(chan error, 1)
	blockedResult <- errors.New("existing result")
	completeReplicationTask(partitionReplicationTask{result: blockedResult}, errors.New("discarded result"))
	require.ErrorContains(t, <-blockedResult, "existing result")
}

func TestCompleteReplicationTaskReleasesMutationBeforeAuthoritativeReconcile(t *testing.T) {
	_, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}}))
	require.Zero(t, partition.GetHWM())

	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		completeReplicationTaskAtHWM(partitionReplicationTask{
			partitionRef: partition, releaseMutation: partition.BeginReplicationMutation(), result: result,
		}, errors.New("post-commit response failed"), 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion reconciled while still holding the replication mutation lock")
	}
	require.ErrorContains(t, <-result, "post-commit response failed")
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
	messages, err := partition.ReadCommitted(0, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
}

func TestPostCommitErrorUsesAuthoritativeTaskHWM(t *testing.T) {
	_, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}}))
	require.Zero(t, partition.GetHWM())

	executor.commitErr = errors.New("local commit observation timed out")
	executor.skipLocalCommit = true
	close(executor.barrier)
	coordinator := newPartitionReplicationCoordinator(1, executor)
	t.Cleanup(coordinator.close)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	task.partitionRef = partition
	task.releaseMutation = partition.BeginReplicationMutation()
	reservation.submit(task)

	require.ErrorContains(t, <-task.result, "local commit observation timed out")
	require.Equal(t, uint64(1), partition.NextOffset(), "post-commit cleanup truncated a committed record")
	require.Equal(t, uint64(1), partition.GetHWM())
	executor.mu.Lock()
	require.Equal(t, 1, executor.replicateCalls, "an accepted commit was retried")
	executor.mu.Unlock()
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
	partition.SetHWM(0)
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
		Messages: []types.Message{{Offset: 0, LeaderEpoch: 8, Payload: "replacement"}},
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
	partition.SetHWM(0)
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

func TestPreparePartitionReplicaAllowsFencedBackfillBelowCommittedHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(topicManager, nil)
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 2, CommittedHWMKnown: true,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, PartitionCount: 1,
	})
	raftManager := &MockRaftManagerForForward{state: state}
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(topicManager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		topicManager.Stop()
		diskManager.CloseAllHandlers()
	})
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)

	release, err := handler.preparePartitionReplica("orders", 0, partition, "broker-1", 7, nil)
	require.NoError(t, err)
	release()
	require.Zero(t, partition.NextOffset())
	catchupBatch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
		Topic: "orders", Partition: 0, BrokerID: "broker-2", StartOffset: 0, CommittedHWM: 2,
		Leader: "broker-1", SourceBroker: "broker-1", LeaderEpoch: 7, LifecycleEpoch: topic.InitialLifecycleEpoch,
		Verified: true, Messages: []types.Message{{Offset: 0, Payload: "zero"}, {Offset: 1, Payload: "one"}},
	})
	require.NoError(t, err)
	require.NoError(t, handler.ApplyReplicaCatchup(context.Background(), catchupBatch))
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())
}

func TestPreparePartitionReplicaUsesLeaderCommittedHWMWhileLocalRaftApplyLags(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(topicManager, nil)
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 0, CommittedHWMKnown: true,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"}, PartitionCount: 1,
	})
	cluster := clusterController.NewClusterController(
		context.Background(), cfg, &MockRaftManagerForForward{state: state}, nil, "broker-2", "broker-2:9001",
	)
	handler := NewCommandHandler(topicManager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		topicManager.Stop()
		diskManager.CloseAllHandlers()
	})
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	messages := []types.Message{{Offset: 0, Payload: "committed"}}
	require.NoError(t, partition.ReplicaAppend(messages))
	require.NoError(t, partition.ApplyReplicaHWM(1))

	leaderCommittedHWM := uint64(1)
	release, err := handler.preparePartitionReplica("orders", 0, partition, "broker-1", 7, &leaderCommittedHWM)
	require.NoError(t, err)
	release()
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
}

func TestApplyReplicaCatchupAcceptsCompactedOffsetRangeAndPreservesHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("state", 1, false, false))
	state := fsm.NewBrokerFSM(topicManager, nil)
	applyPartitionMetadata(t, state, "state", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 5, CommittedHWMKnown: true,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, PartitionCount: 1,
	})
	raftManager := &MockRaftManagerForForward{state: state}
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(topicManager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		diskManager.CloseAllHandlers()
	})

	catchupBatch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
		Topic: "state", Partition: 0, BrokerID: "broker-2", StartOffset: 0, EndOffset: 5,
		CommittedHWM: 5, Leader: "broker-1", SourceBroker: "broker-1", LeaderEpoch: 7,
		LifecycleEpoch: topic.InitialLifecycleEpoch, Compacted: true,
		Verified: true,
		Messages: []types.Message{{Offset: 2, Key: "a", Payload: "current-a"}, {Offset: 4, Key: "b", Payload: "current-b"}},
	})
	require.NoError(t, err)
	require.NoError(t, handler.ApplyReplicaCatchup(context.Background(), catchupBatch))
	partition, err := topicManager.GetTopic("state").GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(5), partition.NextOffset())
	require.Equal(t, uint64(5), partition.GetHWM())
	messages, err := partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4}, []uint64{messages[0].Offset, messages[1].Offset})
	messages, err = partition.ReadCommitted(3, 10)
	require.NoError(t, err, "a committed offset inside a compacted hole must remain in range")
	require.Equal(t, []uint64{4}, []uint64{messages[0].Offset})
}

func TestApplyReplicaCatchupTruncatesDivergentTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(topicManager, nil)
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 8, CommittedHWM: 3, CommittedHWMKnown: true,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, PartitionCount: 1,
	})
	cluster := clusterController.NewClusterController(context.Background(), cfg, &MockRaftManagerForForward{state: state}, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(topicManager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		topicManager.Stop()
		diskManager.CloseAllHandlers()
	})
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.SetHWM(0)
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "common"}, {Payload: "divergent"}}))

	truncateTo := uint64(1)
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
		Topic: "orders", Partition: 0, BrokerID: "broker-2", StartOffset: 2, EndOffset: 2, CommittedHWM: 3,
		Leader: "broker-1", SourceBroker: "broker-1", LeaderEpoch: 8, LifecycleEpoch: topic.InitialLifecycleEpoch,
		TruncateTo: &truncateTo,
	})
	require.NoError(t, err)
	require.NoError(t, handler.ApplyReplicaCatchup(context.Background(), batch))
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
}

func TestDistributedLeaderAcknowledgementHoldsWriteOwnershipUntilReplicationCompletes(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)

	first := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=one", NewClientContext("", 0))
	require.Contains(t, first, `"last_offset":0`)
	<-executor.started
	second := make(chan string, 1)
	go func() {
		second <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=two", NewClientContext("", 0))
	}()
	select {
	case response := <-second:
		t.Fatalf("next append crossed unresolved replication ownership: %s", response)
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, uint64(1), partition.NextOffset(), "next publish appended behind an unresolved tail")
	require.Zero(t, partition.GetHWM())

	close(executor.barrier)
	require.Contains(t, <-second, `"last_offset":1`)
	require.Eventually(t, func() bool { return executor.committed() == 2 }, time.Second, time.Millisecond)
}

func TestDistributedPublishOwnershipWaitHonorsRequestDeadline(t *testing.T) {
	tests := []struct {
		name    string
		publish func(*CommandHandler, *ClientContext) string
	}{
		{
			name: "command",
			publish: func(handler *CommandHandler, clientCtx *ClientContext) string {
				return handler.HandleCommand("PUBLISH topic=orders partition=0 acks=1 producerId=p1 message=value", clientCtx)
			},
		},
		{
			name: "batch",
			publish: func(handler *CommandHandler, clientCtx *ClientContext) string {
				data, err := util.EncodeBatchMessages("orders", 0, "1", false, []types.Message{{Payload: "value", ProducerID: "p1"}})
				require.NoError(t, err)
				response, err := handler.HandleBatchMessage(data, nil, clientCtx)
				require.NoError(t, err)
				return response
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, manager, executor := newDistributedAckTestHandler(t, 2)
			require.NoError(t, manager.CreateTopic("orders", 1, false, false))
			installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})

			first := test.publish(handler, NewClientContext("", 0))
			require.Contains(t, first, `"last_offset":0`)
			<-executor.started

			requestCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			clientCtx := NewClientContext("", 0)
			clientCtx.SetRequestContext(requestCtx)
			started := time.Now()
			second := test.publish(handler, clientCtx)
			require.Equal(t, "ERROR: request_timeout outcome=not_accepted", second)
			require.Less(t, time.Since(started), time.Second)
			partition, err := manager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			require.Equal(t, uint64(1), partition.NextOffset(), "timed-out ownership waiter appended a record")

			close(executor.barrier)
			require.Eventually(t, func() bool { return executor.committed() == 1 }, time.Second, time.Millisecond)
		})
	}
}

func TestDistributedPermanentReplicationFailureRollsBackUncommittedTail(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	executor.replicateErr = permanentReplicationError{}
	executor.replicateFailures = 1
	close(executor.barrier)

	failed := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=failed", NewClientContext("", 0))
	require.Contains(t, failed, "invalid replica append")
	require.Zero(t, partition.NextOffset(), "terminal replication failure left an uncommitted offset reservation")
	require.Zero(t, partition.GetHWM())

	retried := handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=retried", NewClientContext("", 0))
	require.Contains(t, retried, `"last_offset":0`)
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
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

func TestDistributedAllRequestDeadlineReturnsUnknownAndKeepsReplication(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})
	requestCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	clientCtx := NewClientContext("", 0)
	clientCtx.SetRequestContext(requestCtx)
	response := make(chan string, 1)
	go func() {
		response <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=value", clientCtx)
	}()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("replication task did not start")
	}
	select {
	case got := <-response:
		require.Equal(t, "ERROR: request_timeout outcome=unknown", got)
	case <-time.After(time.Second):
		t.Fatal("request did not return after its deadline")
	}
	require.Zero(t, executor.committed(), "timed out replication committed before its worker was released")
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	second := make(chan string, 1)
	go func() {
		second <- handler.HandleCommand("PUBLISH topic=orders partition=0 acks=all producerId=p1 message=next", NewClientContext("", 0))
	}()
	select {
	case got := <-second:
		t.Fatalf("next append crossed timed-out replication ownership: %s", got)
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, uint64(1), partition.NextOffset())

	close(executor.barrier)
	require.Contains(t, <-second, `"last_offset":1`)
	require.Eventually(t, func() bool { return executor.committed() == 2 }, time.Second, time.Millisecond,
		"request timeout canceled replication owned by the broker")
}

func TestAllAcknowledgementKeepsRetryingFollowerTimeoutUntilShutdown(t *testing.T) {
	executor := newBarrierReplicationExecutor()
	executor.replicateErr = context.DeadlineExceeded
	executor.replicateFailures = -1
	coordinator := newPartitionReplicationCoordinator(1, executor)
	reservation, err := coordinator.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(executor, ackpolicy.All)
	reservation.submit(task)
	<-executor.started
	close(executor.barrier)
	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.replicateCalls >= 2
	}, time.Second, time.Millisecond)
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
	require.ErrorIs(t, <-task.result, errReplicationQueueClosed)
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
	executor.state = state
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
	metadata := fmt.Sprintf(`{"leader":"broker-1","leader_epoch":7,"lifecycle_epoch":1,"committed_hwm_version":1,"committed_hwm":0,"replicas":["broker-1","broker-2"],"isr":["%s"],"partition_count":1}`, strings.Join(isr, `","`))
	result := handler.Cluster.RaftManager.GetFSM().Apply(&raft.Log{Data: []byte("PARTITION:" + topicName + "-0:" + metadata)})
	require.Nil(t, result)
}

func requirePartitionOwnershipAvailable(t *testing.T, handler *CommandHandler, partition *topic.Partition) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		releaseWrite, releaseMutation, _, err := handler.preparePartitionLeaderSnapshot("orders", 0, partition, 0)
		if err == nil {
			releaseMutation()
			releaseWrite()
		}
		result <- err
	}()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("partition ownership was not released")
	}
}

func applyPartitionMetadata(t *testing.T, state *fsm.BrokerFSM, topicName string, partition int, metadata fsm.PartitionMetadata) {
	t.Helper()
	metadata.CommittedHWMKnown = true
	if metadata.LifecycleEpoch == 0 {
		metadata.LifecycleEpoch = topic.InitialLifecycleEpoch
	}
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	result := state.Apply(&raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s-%d:%s", topicName, partition, encoded))})
	require.Nil(t, result)
}
