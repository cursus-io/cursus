package transaction

import (
	"fmt"
	"math"
)

// Producer epochs must outlive individual transaction tombstones: partition
// deduplication and visibility decisions can still refer to a retired epoch.
// One monotonic watermark bounds this metadata independently of ID cardinality.
// Gaps (including failed initializations) are safe; reuse is not.
func (m *Manager) allocateProducerEpoch() (int64, error) {
	for {
		next := m.nextProducerEpoch.Load()
		if next > math.MaxInt64 {
			return 0, fmt.Errorf("producer epoch space exhausted")
		}
		if m.nextProducerEpoch.CompareAndSwap(next, next+1) {
			return int64(next), nil // #nosec G115 -- bounded by MaxInt64 above.
		}
	}
}

func (m *Manager) observeProducerEpoch(epoch int64) {
	if epoch >= 0 {
		m.advanceProducerEpoch(uint64(epoch) + 1)
	}
}

func (m *Manager) advanceProducerEpoch(next uint64) {
	for current := m.nextProducerEpoch.Load(); current < next; current = m.nextProducerEpoch.Load() {
		if m.nextProducerEpoch.CompareAndSwap(current, next) {
			return
		}
	}
}

// ValidateProducerEpochWatermark accepts MaxInt64+1 as an exhausted allocator.
func ValidateProducerEpochWatermark(next uint64) error {
	if next > uint64(math.MaxInt64)+1 {
		return fmt.Errorf("invalid next producer epoch %d", next)
	}
	return nil
}

// RestoreProducerEpochWatermark never rolls back allocations already observed
// locally, including allocations made before a failed persistence attempt.
func (m *Manager) RestoreProducerEpochWatermark(next uint64) error {
	if err := ValidateProducerEpochWatermark(next); err != nil {
		return err
	}
	m.advanceProducerEpoch(next)
	return nil
}

// ExportStateWithProducerEpoch captures the watermark and transactions under
// the same locks so pruning cannot erase the only record of an allocated epoch.
func (m *Manager) ExportStateWithProducerEpoch() (map[string]*Snapshot, uint64) {
	m.lockAllShards()
	defer m.unlockAllShards()
	return m.exportStateLocked(), m.nextProducerEpoch.Load()
}
