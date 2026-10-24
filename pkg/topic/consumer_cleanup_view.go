package topic

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
)

type consumerCleanupView struct {
	partitions []*Partition
	bounds     []coordinator.ConsumerCleanupPartition
	ctx        context.Context
}

// CheckConsumerCleanupProof fences a completed in-memory proof at final Raft
// apply without disk I/O or blocking locks. A rollback, replacement or pending
// snapshot requires reconciliation to validate the log again.
func (tm *TopicManager) CheckConsumerCleanupProof(bounds []coordinator.ConsumerCleanupPartition) error {
	invalid := cleanupViewWaiting("completed consumer cleanup proof changed or busy")
	if !tm.mu.TryRLock() {
		return invalid
	}
	defer tm.mu.RUnlock()
	t := tm.topics[config.ConsumerOffsetsTopicName]
	if t == nil || !t.mu.TryRLock() {
		return invalid
	}
	defer t.mu.RUnlock()
	if len(bounds) != len(t.Partitions) {
		return invalid
	}
	for i, p := range t.Partitions {
		b := bounds[i]
		if b.TopicInstance != t.InstanceID || b.TopicEpoch != t.LifecycleEpoch || b.Identity != p || !p.reconcileMu.TryRLock() {
			return invalid
		}
		defer p.reconcileMu.RUnlock()
		if !p.mu.TryRLock() {
			return invalid
		}
		valid := !p.snapshotRecovery && p.hwmReadinessErrorLocked() == nil && p.HWM >= b.End && p.eventIDGeneration.Load() == b.Generation
		p.mu.RUnlock()
		if !valid {
			return invalid
		}
	}
	return nil
}

// WithConsumerCleanupView pins local partition identities and excludes replica
// rollback/snapshot transitions while the callback reads and applies evidence.
// Ordinary appends may continue: the view only exposes the captured committed,
// flushed prefix. Distributed internal metadata compaction is disabled.
func (tm *TopicManager) WithConsumerCleanupView(fn func(coordinator.ConsumerCleanupView) error) error {
	return tm.WithConsumerCleanupViewContext(context.Background(), fn)
}

func (tm *TopicManager) WithConsumerCleanupViewContext(ctx context.Context, fn func(coordinator.ConsumerCleanupView) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !tm.mu.TryRLock() {
		return cleanupViewWaiting("consumer metadata manager busy")
	}
	defer tm.mu.RUnlock()
	t := tm.topics[config.ConsumerOffsetsTopicName]
	if t == nil {
		return cleanupViewWaiting("consumer metadata topic unavailable")
	}
	if !t.mu.TryRLock() {
		return cleanupViewWaiting("consumer metadata topic busy")
	}
	defer t.mu.RUnlock()
	view := &consumerCleanupView{partitions: t.Partitions, ctx: ctx}
	for _, p := range t.Partitions {
		if err := p.reconcileMu.RLockContext(ctx); err != nil {
			return err
		}
		defer p.reconcileMu.RUnlock()
		if err := p.RecoveryError(); err != nil {
			return err
		}
		if err := p.mu.RLockContext(ctx); err != nil {
			return err
		}
		err := p.hwmReadinessErrorLocked()
		pending := p.snapshotRecovery
		end := min(p.HWM, p.dh.GetFlushedOffset())
		p.mu.RUnlock()
		if err != nil {
			return err
		}
		if pending {
			return cleanupViewWaiting("consumer metadata snapshot recovery pending")
		}
		start := p.dh.GetFirstOffset()
		if end < start {
			return cleanupViewWaiting("consumer metadata committed range unavailable")
		}
		var durable string
		if storage, ok := p.dh.(interface {
			CleanupGenerationContext(context.Context) (string, error)
		}); ok {
			token, err := storage.CleanupGenerationContext(ctx)
			if err != nil {
				return err
			}
			durable = fmt.Sprintf("%s:%d:%s", t.InstanceID, t.LifecycleEpoch, token)
		}
		view.bounds = append(view.bounds, coordinator.ConsumerCleanupPartition{
			TopicInstance: t.InstanceID, TopicEpoch: t.LifecycleEpoch,
			DurableGeneration: durable,
			Identity:          p, Generation: p.eventIDGeneration.Load(), Start: start, End: end,
		})
	}
	return fn(view)
}

func (view *consumerCleanupView) Partitions() []coordinator.ConsumerCleanupPartition {
	return view.bounds
}

func (view *consumerCleanupView) Read(partition int, offset uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (coordinator.ConsumerCleanupPage, error) {
	page := coordinator.ConsumerCleanupPage{Next: offset}
	if err := injectedConsumerCleanupReadError(); err != nil {
		return page, err
	}
	if partition < 0 || partition >= len(view.partitions) || maxRecords <= 0 || maxBytes <= 0 {
		return page, fmt.Errorf("invalid consumer metadata page request")
	}
	bounds := view.bounds[partition]
	if offset < bounds.Start || offset >= bounds.End {
		return page, fmt.Errorf("consumer metadata cursor outside committed range")
	}
	p := view.partitions[partition]
	if err := consumerCleanupReadFault(cursor); err != nil {
		return page, err
	}
	reader, ok := p.dh.(types.BoundedReplayReader)
	if !ok {
		return page, fmt.Errorf("bounded physical consumer metadata reader unavailable")
	}
	var err error
	if contextual, ok := p.dh.(interface {
		ReadReplayPageContext(context.Context, uint64, uint64, types.ReplayCursor, int, int) (types.ReplayPage, error)
	}); ok {
		page, err = contextual.ReadReplayPageContext(view.ctx, offset, bounds.End, cursor, maxRecords, maxBytes)
	} else {
		page, err = reader.ReadReplayPage(offset, bounds.End, cursor, maxRecords, maxBytes)
	}
	if err != nil {
		return page, err
	}
	return consumerCleanupEvidenceFault(page, bounds.End)
}

func cleanupViewWaiting(reason string) error {
	return &coordinator.TopicCleanupPending{State: coordinator.CleanupWait, Reason: reason}
}

func (view *consumerCleanupView) PrepareCleanupCheckpoint() error {
	for _, p := range view.partitions {
		storage, ok := p.dh.(interface{ SyncCleanupCheckpointContext(context.Context) error })
		if !ok {
			return fmt.Errorf("durable cleanup checkpoint unsupported")
		}
		if err := storage.SyncCleanupCheckpointContext(view.ctx); err != nil {
			return err
		}
	}
	return nil
}
