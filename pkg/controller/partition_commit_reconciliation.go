package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

// partitionCommitOutcome preserves proof of the original replicated append.
// It grants no permission to recover a different storage or quarantine error.
type partitionCommitOutcome struct {
	cause       error
	command     types.MessageCommand
	snapshot    clusterController.PartitionReplicationSnapshot
	target      uint64
	requiredISR int
}

func (e *partitionCommitOutcome) Error() string {
	return fmt.Sprintf("partition commit outcome unresolved: %v", e.cause)
}
func (e *partitionCommitOutcome) Unwrap() error { return e.cause }

// retainPartitionCommitOutcome owns its proof after the request and queue task
// have completed; callers cannot mutate its record or consumer-route identity.
func retainPartitionCommitOutcome(task partitionReplicationTask, cause error) *partitionCommitOutcome {
	command := task.command
	command.Messages = slices.Clone(command.Messages)
	for i := range command.Messages {
		command.Messages[i].ControlBatchKey = slices.Clone(command.Messages[i].ControlBatchKey)
		command.Messages[i].ControlBatchValue = slices.Clone(command.Messages[i].ControlBatchValue)
	}
	if command.ConsumerRoute != nil {
		route := *command.ConsumerRoute
		if route.Transaction != nil {
			transaction := *route.Transaction
			route.Transaction = &transaction
		}
		command.ConsumerRoute = &route
	}
	if command.CommitHWM != nil {
		hwm := *command.CommitHWM
		command.CommitHWM = &hwm
	}
	snapshot := task.snapshot
	snapshot.ISR = slices.Clone(snapshot.ISR)
	snapshot.Replicas = slices.Clone(snapshot.Replicas)
	snapshot.RecoveryReplicas = slices.Clone(snapshot.RecoveryReplicas)
	return &partitionCommitOutcome{cause: cause, command: command, snapshot: snapshot, target: task.commitHWM, requiredISR: task.requiredISR}
}

// reconcilePartitionCommitLocked resumes only a proven original batch. The
// caller owns the partition write lock; mutation ownership protects its bytes.
// Each request makes one bounded attempt, without appending or truncating data.
func (ch *CommandHandler) reconcilePartitionCommitLocked(ctx context.Context, name string, id int, p *topic.Partition, current clusterController.PartitionReplicationSnapshot, minISR int) (bool, error) {
	marker := p.ReconciliationError()
	var pending *partitionCommitOutcome
	if !errors.As(marker, &pending) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !sameReplicationFence(current, pending.snapshot) || !sameBrokerSet(current.Replicas, pending.snapshot.Replicas) ||
		len(current.RecoveryReplicas) != 0 || len(current.ISR) < max(minISR, pending.requiredISR) {
		return false, fmt.Errorf("partition commit reconciliation fence or replica set changed")
	}
	command := pending.command
	if command.Topic != name || command.Partition != id || len(command.Messages) == 0 ||
		p.NextOffset() != pending.target || current.CommittedHWM > pending.target ||
		current.CommittedHWM < command.Messages[0].Offset || p.GetHWM() > current.CommittedHWM {
		return false, fmt.Errorf("partition commit reconciliation boundary does not match retained batch")
	}
	if err := ch.Cluster.RaftManager.GetFSM().ValidateConsumerMetadataRoute(name, id, command.ConsumerRoute); err != nil {
		return false, err
	}
	release := p.BeginReplicationMutation()
	defer release()
	next := command.Messages[0].Offset
	for _, message := range command.Messages {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if message.Offset != next || next == math.MaxUint64 || message.LeaderEpoch != int64(current.LeaderEpoch) {
			return false, fmt.Errorf("partition commit reconciliation record fence or sequence changed")
		}
		if err := p.ValidateReconciliationRecord(marker, message); err != nil {
			return false, err
		}
		next++
	}
	if next != pending.target {
		return false, fmt.Errorf("partition commit reconciliation batch does not reach target")
	}
	task := partitionReplicationTask{topic: name, partition: id, command: command, snapshot: current,
		partitionRef: p, commitHWM: pending.target}
	executor := clusterPartitionReplicationExecutor{handler: ch}
	if err := executor.ReplicateISR(ctx, task, current); err != nil {
		return false, err
	}
	latest, err := executor.Snapshot(name, id)
	if err != nil {
		return false, err
	}
	if !sameReplicationFence(current, latest) || !sameBrokerSet(current.ISR, latest.ISR) ||
		!sameBrokerSet(current.Replicas, latest.Replicas) || len(latest.RecoveryReplicas) != 0 {
		return false, fmt.Errorf("partition commit reconciliation fence changed before commit")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := executor.Commit(task); err != nil {
		return false, err
	}
	if err := p.CompleteReconciliation(marker, pending.target); err != nil {
		return false, err
	}
	return true, nil
}
