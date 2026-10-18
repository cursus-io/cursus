package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

// streamAppendOutcome retains the exact append and fence after an ambiguous
// reply. It is not permission to clear unrelated storage/recovery errors.
type streamAppendOutcome struct {
	cause    error
	stage    string
	message  types.Message
	snapshot clusterController.PartitionReplicationSnapshot
}

func (e *streamAppendOutcome) Error() string {
	return fmt.Sprintf("stream %s outcome requires reconciliation: %v", e.stage, e.cause)
}

func (e *streamAppendOutcome) Unwrap() error { return e.cause }

// reconcileStreamAppendLocked completes the same durable append, never a new
// offset. The caller owns the partition write lock; the mutation guard excludes
// snapshot/truncation while the original bytes are re-replicated and committed.
// Each producer retry makes one bounded replication attempt, without a new loop.
func (ch *CommandHandler) reconcileStreamAppendLocked(ctx context.Context, name string, id int, p *topic.Partition, current clusterController.PartitionReplicationSnapshot, minISR int) (bool, error) {
	t := ch.TopicManager.GetTopic(name)
	if t == nil || !t.IsEventSourcing {
		return false, nil
	}
	marker := p.ReconciliationError()
	var pending *streamAppendOutcome
	if !errors.As(marker, &pending) {
		if p.RecoveryError() != nil || p.NextOffset() <= current.CommittedHWM {
			return false, nil
		}
		// A surviving single-record tail carries its original leader epoch in
		// the existing log. Do not truncate/reuse it just because the in-memory
		// outcome marker was lost (for example when rebuilding a handler).
		pending = &streamAppendOutcome{stage: "retained append", snapshot: current}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !sameReplicationFence(current, pending.snapshot) ||
		!sameBrokerSet(current.ISR, pending.snapshot.ISR) ||
		!sameBrokerSet(current.Replicas, pending.snapshot.Replicas) ||
		len(current.RecoveryReplicas) != 0 || len(current.ISR) < minISR {
		return false, fmt.Errorf("stream reconciliation fence or replica set changed")
	}
	release := p.BeginReplicationMutation()
	defer release()
	if marker == nil {
		// Do not return (false, nil) for unsupported retained stream tails:
		// preparation would then call ReconcileCommittedHWM and truncate them.
		// Preserving uncertain events takes precedence over write availability;
		// old-epoch/multiple-record tails require explicit recovery instead.
		if current.CommittedHWM == math.MaxUint64 || p.NextOffset() != current.CommittedHWM+1 {
			return false, fmt.Errorf("stream reconciliation requires a single retained tail record")
		}
		messages, err := p.ReadMessages(current.CommittedHWM, 1)
		if err != nil || len(messages) != 1 {
			return false, fmt.Errorf("stream reconciliation retained record unavailable: %v", err)
		}
		pending.message = messages[0]
	}
	message := pending.message
	if message.Offset == math.MaxUint64 || message.LeaderEpoch != int64(current.LeaderEpoch) ||
		message.Key == "" || message.AggregateVersion == 0 || message.TransactionalID != "" ||
		message.TransactionMarker != "" || message.ControlBatchType != "" {
		return false, fmt.Errorf("stream reconciliation record is not a same-epoch stream append")
	}
	target := message.Offset + 1
	if p.NextOffset() != target || current.CommittedHWM < message.Offset || current.CommittedHWM > target {
		return false, fmt.Errorf("stream reconciliation watermark does not match retained append")
	}
	if err := p.ValidateReconciliationRecord(marker, message); err != nil {
		return false, err
	}
	if marker == nil {
		if err := ch.validateRetainedStreamAppend(ctx, name, p, current, message); err != nil {
			return false, err
		}
		p.MarkReconciliationPending(pending)
		marker = p.ReconciliationError()
	}
	task := partitionReplicationTask{
		topic: name, partition: id, partitionRef: p, snapshot: current, commitHWM: target,
		command: types.MessageCommand{Topic: name, Partition: id, LifecycleEpoch: current.LifecycleEpoch,
			Messages: []types.Message{message}, Acks: "all", SequenceScope: "partition"},
	}
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
		return false, fmt.Errorf("stream reconciliation fence changed before commit")
	}
	if _, err := executor.Commit(task); err != nil {
		return false, err
	}
	// Commit waits for local Raft application. Release only the exact marker
	// that was validated, and only after the existing record is committed.
	if err := p.CompleteReconciliation(marker, target); err != nil {
		return false, err
	}
	return true, nil
}

// validateRetainedStreamAppend rebuilds proof from the committed prefix when
// there is no in-memory append outcome. A same-epoch record alone is not proof
// of stream version or global EventID uniqueness after reopening storage.
func (ch *CommandHandler) validateRetainedStreamAppend(ctx context.Context, name string, p *topic.Partition, snapshot clusterController.PartitionReplicationSnapshot, message types.Message) error {
	if p.GetHWM() != snapshot.CommittedHWM || ch.ESHandler == nil {
		return fmt.Errorf("stream reconciliation committed prefix is not available")
	}
	if response := ch.reconcileEventSourceIndex(name, p.ID()); response != "" {
		return fmt.Errorf("stream reconciliation index unavailable: %s", response)
	}
	version, _, err := ch.ESHandler.CurrentVersion(name, message.Key)
	if err != nil || version == math.MaxUint64 || version+1 != message.AggregateVersion {
		return fmt.Errorf("stream reconciliation aggregate version conflicts with committed prefix")
	}
	if !ch.TopicManager.GetTopic(name).PolicySnapshot().AggregateReplay {
		return nil
	}
	digest := sha256.Sum256([]byte(message.Payload))
	if message.EventID == "" || message.PayloadDigest != hex.EncodeToString(digest[:]) || message.ProducerID == "" || message.SeqNum == 0 {
		return fmt.Errorf("stream reconciliation aggregate identity is invalid")
	}
	for offset := p.FirstOffset(); offset < snapshot.CommittedHWM; {
		if err := ctx.Err(); err != nil {
			return err
		}
		messages, _, next, err := p.ReadCommittedPage(offset, 256, 16<<20, true)
		if err != nil {
			return err
		}
		if next <= offset {
			return fmt.Errorf("stream reconciliation committed identity proof incomplete")
		}
		for _, existing := range messages {
			if existing.EventID == message.EventID {
				return fmt.Errorf("stream reconciliation aggregate identity conflicts with committed prefix")
			}
		}
		offset = next
	}
	return nil
}
