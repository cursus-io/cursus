package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

var errReplicationQueueClosed = errors.New("replication queue closed")

type partitionReplicationTask struct {
	topic           string
	partition       int
	command         types.MessageCommand
	commitHWM       uint64
	ackMode         ackpolicy.Mode
	requiredISR     int
	duplicate       bool
	snapshot        clusterController.PartitionReplicationSnapshot
	partitionRef    *topic.Partition
	releaseMutation func()
	releaseWrite    func()
	result          chan error
}

type retryableReplicationStateError struct {
	message string
	class   string
}

func (e *retryableReplicationStateError) Error() string { return e.message }

func (e *retryableReplicationStateError) Retryable() bool { return true }

func (e *retryableReplicationStateError) ReplicationErrorClass() string { return e.class }

type partitionReplicationExecutor interface {
	Snapshot(topic string, partition int) (clusterController.PartitionReplicationSnapshot, error)
	ReplicateISR(ctx context.Context, task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error
	RecoverReplicaGap(task partitionReplicationTask, cause error) (replicaGapRecoveryResult, error)
	Commit(task partitionReplicationTask) (partitionCommitResult, error)
}

type replicaGapRecoveryResult struct {
	resolved            bool
	authoritativeHWM    uint64
	hasAuthoritativeHWM bool
}

type partitionCommitResult struct {
	accepted bool
	hwm      uint64
}

type clusterPartitionReplicationExecutor struct {
	handler *CommandHandler
}

func (e clusterPartitionReplicationExecutor) Snapshot(topicName string, partition int) (clusterController.PartitionReplicationSnapshot, error) {
	return e.handler.Cluster.GetPartitionReplicationSnapshot(topicName, partition)
}

func (e clusterPartitionReplicationExecutor) ReplicateISR(ctx context.Context, task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.handler.Cluster.ReplicateToISR(task.topic, task.partition, task.command, snapshot)
}

func (e clusterPartitionReplicationExecutor) RecoverReplicaGap(task partitionReplicationTask, cause error) (replicaGapRecoveryResult, error) {
	var gap interface {
		ReplicationErrorCode() string
		ReplicaBrokerID() string
	}
	if !errors.As(cause, &gap) || gap.ReplicationErrorCode() != "replica_offset_gap" {
		return replicaGapRecoveryResult{}, cause
	}
	metadata := e.handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata(fmt.Sprintf("%s-%d", task.topic, task.partition))
	if metadata == nil || !metadata.CommittedHWMKnown {
		return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("authoritative committed HWM unavailable before replica quarantine")
	}
	_, err := e.handler.applyViaLeader("ISR_QUARANTINE", map[string]interface{}{
		"topic": task.topic, "partition": task.partition, "broker_id": gap.ReplicaBrokerID(),
		"leader": task.snapshot.Leader, "leader_epoch": task.snapshot.LeaderEpoch,
		"lifecycle_epoch": task.snapshot.LifecycleEpoch, "committed_hwm": metadata.CommittedHWM,
		"expected_isr": append([]string(nil), metadata.ISR...), "expected_replicas": append([]string(nil), metadata.Replicas...),
	})
	if err != nil {
		return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("quarantine divergent replica %s: %w", gap.ReplicaBrokerID(), err)
	}
	key := fmt.Sprintf("%s-%d", task.topic, task.partition)
	deadline := time.Now().Add(DefaultFSMApplyTimeout)
	for {
		metadata = e.handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata(key)
		if metadata != nil && metadata.Leader == task.snapshot.Leader && metadata.LeaderEpoch == task.snapshot.LeaderEpoch &&
			metadata.LifecycleEpoch == task.snapshot.LifecycleEpoch && containsReplica(metadata.RecoveryReplicas, gap.ReplicaBrokerID()) &&
			!containsReplica(metadata.ISR, gap.ReplicaBrokerID()) {
			break
		}
		if !time.Now().Before(deadline) {
			return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("local Raft state did not apply replica quarantine for %s before recovery decision", gap.ReplicaBrokerID())
		}
		time.Sleep(5 * time.Millisecond)
	}
	current, err := e.Snapshot(task.topic, task.partition)
	if err != nil {
		return replicaGapRecoveryResult{resolved: true}, err
	}
	metadata = e.handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata(key)
	if metadata == nil || !metadata.CommittedHWMKnown {
		return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("authoritative committed HWM unavailable after replica quarantine")
	}
	if metadata.CommittedHWM >= task.commitHWM {
		if task.partitionRef == nil {
			return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("local partition unavailable after replica quarantine")
		}
		return replicaGapRecoveryResult{
			resolved: true, authoritativeHWM: metadata.CommittedHWM, hasAuthoritativeHWM: true,
		}, nil
	}
	if task.ackMode == ackpolicy.All && len(current.ISR) < task.requiredISR {
		return replicaGapRecoveryResult{resolved: true}, fmt.Errorf("insufficient in-sync replicas after gap quarantine: got %d, want minISR %d", len(current.ISR), task.requiredISR)
	}
	return replicaGapRecoveryResult{}, nil
}

func (e clusterPartitionReplicationExecutor) Commit(task partitionReplicationTask) (partitionCommitResult, error) {
	result := partitionCommitResult{hwm: task.commitHWM}
	if err := e.handler.commitPartitionHWMAtEpoch(
		task.topic,
		task.partition,
		task.commitHWM,
		task.snapshot.Leader,
		task.snapshot.LeaderEpoch,
		task.snapshot.LifecycleEpoch,
	); err != nil {
		metadata := e.handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata(fmt.Sprintf("%s-%d", task.topic, task.partition))
		result.accepted = metadata != nil && metadata.Leader == task.snapshot.Leader &&
			metadata.LeaderEpoch == task.snapshot.LeaderEpoch && metadata.LifecycleEpoch == task.snapshot.LifecycleEpoch &&
			metadata.CommittedHWMKnown && metadata.CommittedHWM >= task.commitHWM
		return result, err
	}
	result.accepted = true
	deadline := time.Now().Add(DefaultFSMApplyTimeout)
	key := fmt.Sprintf("%s-%d", task.topic, task.partition)
	for {
		metadata := e.handler.Cluster.RaftManager.GetFSM().GetPartitionMetadata(key)
		if metadata != nil && metadata.Leader == task.snapshot.Leader && metadata.LeaderEpoch == task.snapshot.LeaderEpoch &&
			metadata.LifecycleEpoch == task.snapshot.LifecycleEpoch && metadata.CommittedHWMKnown && metadata.CommittedHWM >= task.commitHWM {
			break
		}
		if !time.Now().Before(deadline) {
			return result, fmt.Errorf("local Raft state did not apply committed HWM %d before replication ownership release", task.commitHWM)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := task.partitionRef.ApplyReplicaHWM(task.commitHWM); err != nil {
		return result, fmt.Errorf("apply local commit watermark: %w", err)
	}
	task.partitionRef.FlushDisk()
	return result, nil
}

type partitionReplicationCoordinator struct {
	ctx            context.Context
	cancel         context.CancelFunc
	reserveCtx     context.Context
	cancelReserves context.CancelFunc
	capacity       int
	executor       partitionReplicationExecutor
	mu             sync.Mutex
	closed         bool
	lanes          map[string]*partitionReplicationLane
	submissions    sync.WaitGroup
	workers        sync.WaitGroup
}

type partitionReplicationLane struct {
	owner *partitionReplicationCoordinator
	queue chan partitionReplicationTask
	slots chan struct{}
}

type partitionReplicationReservation struct {
	lane *partitionReplicationLane
	once sync.Once
}

func newPartitionReplicationCoordinator(capacity int, executor partitionReplicationExecutor) *partitionReplicationCoordinator {
	if capacity <= 0 {
		capacity = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	reserveCtx, cancelReserves := context.WithCancel(context.Background())
	return &partitionReplicationCoordinator{
		ctx:            ctx,
		cancel:         cancel,
		reserveCtx:     reserveCtx,
		cancelReserves: cancelReserves,
		capacity:       capacity,
		executor:       executor,
		lanes:          make(map[string]*partitionReplicationLane),
	}
}

func (c *partitionReplicationCoordinator) reserve(ctx context.Context, topicName string, partition int) (*partitionReplicationReservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errReplicationQueueClosed
	}
	key := fmt.Sprintf("%s-%d", topicName, partition)
	lane := c.lanes[key]
	if lane == nil {
		lane = &partitionReplicationLane{
			owner: c,
			queue: make(chan partitionReplicationTask, c.capacity),
			slots: make(chan struct{}, c.capacity),
		}
		c.lanes[key] = lane
		c.workers.Add(1)
		go lane.run()
	}
	c.submissions.Add(1)
	c.mu.Unlock()

	select {
	case lane.slots <- struct{}{}:
		return &partitionReplicationReservation{lane: lane}, nil
	case <-ctx.Done():
		c.submissions.Done()
		return nil, ctx.Err()
	case <-c.reserveCtx.Done():
		c.submissions.Done()
		return nil, errReplicationQueueClosed
	}
}

func (r *partitionReplicationReservation) submit(task partitionReplicationTask) {
	r.once.Do(func() {
		r.lane.queue <- task
		r.lane.owner.submissions.Done()
	})
}

func (r *partitionReplicationReservation) release() {
	if r == nil || r.lane == nil {
		return
	}
	r.once.Do(func() {
		<-r.lane.slots
		r.lane.owner.submissions.Done()
	})
}

func (c *partitionReplicationCoordinator) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	c.cancelReserves()
	c.submissions.Wait()
	c.cancel()
	c.workers.Wait()
}

func (l *partitionReplicationLane) run() {
	defer l.owner.workers.Done()
	for {
		select {
		case task := <-l.queue:
			if l.owner.ctx.Err() != nil {
				completeReplicationTask(task, errReplicationQueueClosed)
				<-l.slots
				continue
			}
			l.process(task)
			<-l.slots
		case <-l.owner.ctx.Done():
			for {
				select {
				case task := <-l.queue:
					completeReplicationTask(task, errReplicationQueueClosed)
					<-l.slots
				default:
					return
				}
			}
		}
	}
}

func (l *partitionReplicationLane) process(task partitionReplicationTask) {
	backoff := 25 * time.Millisecond
	failures := uint64(0)
	var transactionStateLagSince time.Time
	acknowledgedRecoveryPending := false
	var pendingGapCause error
	waitForGapDecision := func(cause, pendingErr error) bool {
		if pendingErr == nil {
			pendingErr = errors.New("replica gap recovery decision has no authoritative committed HWM")
		}
		if task.partitionRef != nil {
			task.partitionRef.MarkReconciliationPending(pendingErr)
		}
		acknowledgedRecoveryPending = true
		failures++
		metrics.ReplicationRetries.WithLabelValues(task.topic, string(task.ackMode), "replica_offset_gap_recovery").Inc()
		if failures == 1 || failures&(failures-1) == 0 {
			util.Error("acknowledged partition replication awaiting replica gap decision topic=%s partition=%d broker=%s attempt=%d error=%v", task.topic, task.partition, replicaGapBrokerID(cause), failures, pendingErr)
		}
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, time.Second)
			return true
		case <-l.owner.ctx.Done():
			completeReplicationTaskPreservingTail(task, errReplicationQueueClosed)
			return false
		}
	}
	for {
		if l.owner.ctx.Err() != nil {
			if acknowledgedRecoveryPending {
				completeReplicationTaskPreservingTail(task, errReplicationQueueClosed)
			} else {
				completeReplicationTask(task, errReplicationQueueClosed)
			}
			return
		}
		if pendingGapCause != nil {
			recovery, recoveryErr := l.owner.executor.RecoverReplicaGap(task, pendingGapCause)
			if recovery.hasAuthoritativeHWM {
				completeReplicationTaskAtHWM(task, recoveryErr, recovery.authoritativeHWM)
				return
			}
			if recoveryErr == nil && !recovery.resolved {
				pendingGapCause = nil
				backoff = 25 * time.Millisecond
				continue
			}
			if !waitForGapDecision(pendingGapCause, recoveryErr) {
				return
			}
			continue
		}

		snapshot, err := l.replicationSnapshot(task)
		if err == nil && task.duplicate {
			if task.partitionRef == nil {
				err = errors.New("duplicate producer sequence partition is unavailable")
			} else {
				committedHWM := task.partitionRef.GetHWM()
				if committedHWM >= task.commitHWM {
					completeReplicationTask(task, nil)
					if failures > 0 {
						util.Info("partition replication recovered topic=%s partition=%d ack_mode=%s attempts=%d", task.topic, task.partition, task.ackMode, failures+1)
					}
					return
				}
			}
		}
		if err == nil {
			err = l.owner.executor.ReplicateISR(l.owner.ctx, task, snapshot)
		}
		if err == nil {
			current, snapshotErr := l.owner.executor.Snapshot(task.topic, task.partition)
			if snapshotErr != nil {
				err = snapshotErr
			} else if !sameReplicationFence(current, task.snapshot) {
				err = fmt.Errorf("%w before commit: current=%s/%d/%d requested=%s/%d/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, current.LifecycleEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch, task.snapshot.LifecycleEpoch)
			} else if task.ackMode == ackpolicy.All && !sameBrokerSet(current.ISR, snapshot.ISR) {
				err = &retryableReplicationStateError{
					message: fmt.Sprintf("ISR changed during replication: current=%v replicated=%v", current.ISR, snapshot.ISR),
					class:   "insufficient_isr",
				}
			}
		}
		commitResult := partitionCommitResult{}
		if err == nil {
			commitResult, err = l.owner.executor.Commit(task)
		}
		if err == nil {
			current, snapshotErr := l.owner.executor.Snapshot(task.topic, task.partition)
			if snapshotErr != nil {
				err = snapshotErr
			} else if !sameReplicationFence(current, task.snapshot) {
				err = fmt.Errorf("%w after commit: current=%s/%d/%d requested=%s/%d/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, current.LifecycleEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch, task.snapshot.LifecycleEpoch)
			}
		}
		if err == nil {
			if acknowledgedRecoveryPending {
				completeReplicationTaskAtHWM(task, nil, task.commitHWM)
			} else {
				completeReplicationTask(task, nil)
			}
			if failures > 0 {
				util.Info("partition replication recovered topic=%s partition=%d ack_mode=%s attempts=%d", task.topic, task.partition, task.ackMode, failures+1)
			}
			return
		}
		if commitResult.accepted {
			completeReplicationTaskAtHWM(task, err, commitResult.hwm)
			return
		}
		if l.owner.ctx.Err() != nil {
			if acknowledgedRecoveryPending {
				completeReplicationTaskPreservingTail(task, errReplicationQueueClosed)
			} else {
				completeReplicationTask(task, errReplicationQueueClosed)
			}
			return
		}
		if replicaGapError(err) {
			recovery, recoveryErr := l.owner.executor.RecoverReplicaGap(task, err)
			if task.ackMode == ackpolicy.Leader && !recovery.hasAuthoritativeHWM && (recovery.resolved || recoveryErr != nil) {
				pendingGapCause = err
				if !waitForGapDecision(err, recoveryErr) {
					return
				}
				continue
			}
			if recovery.resolved {
				if recovery.hasAuthoritativeHWM {
					completeReplicationTaskAtHWM(task, recoveryErr, recovery.authoritativeHWM)
				} else {
					completeReplicationTask(task, recoveryErr)
				}
				return
			}
			if recoveryErr != nil {
				completeReplicationTask(task, recoveryErr)
				return
			}
			failures++
			metrics.ReplicationRetries.WithLabelValues(task.topic, string(task.ackMode), "replica_offset_gap").Inc()
			util.Error("partition replica quarantined after offset gap topic=%s partition=%d broker=%s ack_mode=%s error=%v", task.topic, task.partition, replicaGapBrokerID(err), task.ackMode, err)
			backoff = 25 * time.Millisecond
			continue
		}

		class := replicationErrorClass(err)
		if isReplicationFenceError(err) {
			util.Error("partition replication fenced topic=%s partition=%d ack_mode=%s error_class=%s error=%v", task.topic, task.partition, task.ackMode, class, err)
			if acknowledgedRecoveryPending {
				completeReplicationTaskPreservingTail(task, err)
			} else {
				completeReplicationTask(task, err)
			}
			return
		}
		retryTransactionStateLag := false
		if replicaTransactionStateLag(task, err) {
			if transactionStateLagSince.IsZero() {
				transactionStateLagSince = time.Now()
			}
			retryTransactionStateLag = time.Since(transactionStateLagSince) < DefaultFSMApplyTimeout
			if retryTransactionStateLag {
				class = "transaction_state_lag"
			}
		}
		if !retryTransactionStateLag && !isRetryableReplicationError(err) {
			util.Error("partition replication failed permanently topic=%s partition=%d ack_mode=%s error_class=%s error=%v", task.topic, task.partition, task.ackMode, class, err)
			if acknowledgedRecoveryPending {
				completeReplicationTaskPreservingTail(task, err)
			} else {
				completeReplicationTask(task, err)
			}
			return
		}
		failures++
		metrics.ReplicationRetries.WithLabelValues(task.topic, string(task.ackMode), class).Inc()
		if task.ackMode != ackpolicy.All {
			metrics.AsyncReplicationFailures.WithLabelValues(task.topic, class).Inc()
		}
		if failures == 1 || failures&(failures-1) == 0 {
			util.Error("partition replication retrying topic=%s partition=%d ack_mode=%s attempt=%d error_class=%s error=%v", task.topic, task.partition, task.ackMode, failures, class, err)
		}
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, time.Second)
		case <-l.owner.ctx.Done():
			if acknowledgedRecoveryPending {
				completeReplicationTaskPreservingTail(task, errReplicationQueueClosed)
			} else {
				completeReplicationTask(task, errReplicationQueueClosed)
			}
			return
		}
	}
}

// A committed TXN_SYNC may reach the partition leader before another ISR
// replica has applied it. Only that replica's staged-record response is a
// transient replication error, and the lane retries it for a bounded window.
func replicaTransactionStateLag(task partitionReplicationTask, err error) bool {
	var replicaErr interface {
		ReplicationErrorCode() string
		ReplicaBrokerID() string
	}
	if !errors.As(err, &replicaErr) || replicaErr.ReplicationErrorCode() != "transaction_record_not_staged" || replicaErr.ReplicaBrokerID() == "" || len(task.command.Messages) == 0 {
		return false
	}
	transactionalID := task.command.Messages[0].TransactionalID
	if transactionalID == "" {
		return false
	}
	for _, message := range task.command.Messages[1:] {
		if message.TransactionalID != transactionalID {
			return false
		}
	}
	return true
}

func replicaGapError(err error) bool {
	var classified interface{ ReplicationErrorCode() string }
	return errors.As(err, &classified) && classified.ReplicationErrorCode() == "replica_offset_gap"
}

func replicaGapBrokerID(err error) string {
	var classified interface{ ReplicaBrokerID() string }
	if errors.As(err, &classified) {
		return classified.ReplicaBrokerID()
	}
	return "unknown"
}

func (l *partitionReplicationLane) replicationSnapshot(task partitionReplicationTask) (clusterController.PartitionReplicationSnapshot, error) {
	current, err := l.owner.executor.Snapshot(task.topic, task.partition)
	if err != nil {
		return clusterController.PartitionReplicationSnapshot{}, err
	}
	if !sameReplicationFence(current, task.snapshot) {
		return clusterController.PartitionReplicationSnapshot{}, fmt.Errorf("%w: current=%s/%d/%d requested=%s/%d/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, current.LifecycleEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch, task.snapshot.LifecycleEpoch)
	}
	if task.ackMode == ackpolicy.All {
		if len(current.ISR) < task.requiredISR {
			return clusterController.PartitionReplicationSnapshot{}, &retryableReplicationStateError{
				message: fmt.Sprintf("insufficient in-sync replicas: got %d, want minISR %d", len(current.ISR), task.requiredISR),
				class:   "insufficient_isr",
			}
		}
		return current, nil
	}
	return current, nil
}

func sameReplicationFence(left, right clusterController.PartitionReplicationSnapshot) bool {
	return left.Leader == right.Leader &&
		left.LeaderEpoch == right.LeaderEpoch &&
		left.LifecycleEpoch == right.LifecycleEpoch
}

func sameBrokerSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	members := make(map[string]struct{}, len(left))
	for _, brokerID := range left {
		members[brokerID] = struct{}{}
	}
	for _, brokerID := range right {
		if _, found := members[brokerID]; !found {
			return false
		}
	}
	return true
}

func completeReplicationTask(task partitionReplicationTask, err error) {
	completeReplicationTaskWithHWM(task, err, 0, false)
}

func completeReplicationTaskAtHWM(task partitionReplicationTask, err error, authoritativeHWM uint64) {
	completeReplicationTaskWithHWM(task, err, authoritativeHWM, true)
}

// completeReplicationTaskPreservingTail releases ownership without rolling an
// already acknowledged append back to the previous local HWM. The recovery
// marker remains set so readiness and future leader writes stay fail-closed
// until verified recovery reconciles an authoritative committed HWM.
func completeReplicationTaskPreservingTail(task partitionReplicationTask, err error) {
	if task.releaseMutation != nil {
		task.releaseMutation()
	}
	if task.releaseWrite != nil {
		task.releaseWrite()
	}
	if task.result == nil {
		return
	}
	select {
	case task.result <- err:
	default:
	}
}

func completeReplicationTaskWithHWM(task partitionReplicationTask, err error, authoritativeHWM uint64, hasAuthoritativeHWM bool) {
	if task.releaseMutation != nil {
		task.releaseMutation()
	}
	if (err != nil || hasAuthoritativeHWM) && task.partitionRef != nil {
		committedHWM := task.partitionRef.GetHWM()
		if hasAuthoritativeHWM {
			committedHWM = authoritativeHWM
		}
		if reconcileErr := task.partitionRef.ReconcileCommittedHWM(committedHWM); reconcileErr != nil {
			task.partitionRef.MarkReconciliationPending(reconcileErr)
			err = errors.Join(err, fmt.Errorf("reconcile failed replication to committed HWM %d: %w", committedHWM, reconcileErr))
		} else {
			task.partitionRef.FlushDisk()
		}
	}
	if task.releaseWrite != nil {
		task.releaseWrite()
	}
	if task.result == nil {
		return
	}
	select {
	case task.result <- err:
	default:
	}
}

func isReplicationFenceError(err error) bool {
	return errors.Is(err, clusterController.ErrPartitionLeaderFenced)
}

type classifiedReplicationError interface {
	Retryable() bool
	ReplicationErrorClass() string
}

func isRetryableReplicationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var classified classifiedReplicationError
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}

func replicationErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	var classified classifiedReplicationError
	if errors.As(err, &classified) {
		return classified.ReplicationErrorClass()
	}
	value := strings.ToLower(err.Error())
	switch {
	case isReplicationFenceError(err):
		return "fenced"
	case strings.Contains(value, "in-sync") || strings.Contains(value, "isr"):
		return "insufficient_isr"
	case strings.Contains(value, "timeout") || strings.Contains(value, "deadline"):
		return "timeout"
	case strings.Contains(value, "cancel"):
		return "cancelled"
	case errors.Is(err, errReplicationQueueClosed):
		return "shutdown"
	default:
		return "replication"
	}
}
