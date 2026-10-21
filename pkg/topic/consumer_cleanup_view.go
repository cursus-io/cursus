package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
)

type consumerCleanupView struct {
	partitions []*Partition
	bounds     []coordinator.ConsumerCleanupPartition
}

// WithConsumerCleanupView pins local partition identities and excludes replica
// rollback/snapshot transitions while the callback reads and applies evidence.
// Ordinary appends may continue: the view only exposes the captured committed,
// flushed prefix. Distributed internal metadata compaction is disabled.
func (tm *TopicManager) WithConsumerCleanupView(fn func(coordinator.ConsumerCleanupView) error) error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t := tm.topics[config.ConsumerOffsetsTopicName]
	if t == nil {
		return fmt.Errorf("consumer metadata topic unavailable")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	view := &consumerCleanupView{partitions: t.Partitions}
	for _, p := range t.Partitions {
		p.reconcileMu.RLock()
		defer p.reconcileMu.RUnlock()
		if err := p.RecoveryError(); err != nil {
			return err
		}
		p.mu.RLock()
		err := p.hwmReadinessErrorLocked()
		pending := p.snapshotRecovery
		end := min(p.HWM, p.dh.GetFlushedOffset())
		p.mu.RUnlock()
		if err != nil {
			return err
		}
		if pending {
			return fmt.Errorf("consumer metadata snapshot recovery pending")
		}
		start := p.dh.GetFirstOffset()
		if end < start {
			return fmt.Errorf("consumer metadata committed range unavailable")
		}
		view.bounds = append(view.bounds, coordinator.ConsumerCleanupPartition{
			Identity: p, Generation: p.eventIDGeneration.Load(), Start: start, End: end,
		})
	}
	return fn(view)
}

func (view *consumerCleanupView) Partitions() []coordinator.ConsumerCleanupPartition {
	return view.bounds
}

func (view *consumerCleanupView) Read(partition int, offset uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (coordinator.ConsumerCleanupPage, error) {
	page := coordinator.ConsumerCleanupPage{Next: offset}
	if partition < 0 || partition >= len(view.partitions) || maxRecords <= 0 || maxBytes <= 0 {
		return page, fmt.Errorf("invalid consumer metadata page request")
	}
	bounds := view.bounds[partition]
	if offset < bounds.Start || offset >= bounds.End {
		return page, fmt.Errorf("consumer metadata cursor outside committed range")
	}
	p := view.partitions[partition]
	reader, ok := p.dh.(types.BoundedReplayReader)
	if !ok {
		return page, fmt.Errorf("bounded physical consumer metadata reader unavailable")
	}
	return reader.ReadReplayPage(offset, bounds.End, cursor, maxRecords, maxBytes)
}
