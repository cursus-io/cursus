package topic

import (
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// stampAppendTimeLocked only runs for new leader/standalone appends. Replica
// paths preserve their source bytes and must never call it. Client provenance
// is unconditionally discarded, even before activation.
func (p *Partition) stampAppendTimeLocked(msg *types.Message) error {
	msg.AppendTimeMS = 0
	activated := p.retentionTimeGate != nil && p.retentionTimeGate()
	if !p.retentionTimeEnabled && !activated {
		return nil
	}
	if p.distributed && !activated {
		return fmt.Errorf("durable retention time capability is not activated")
	}
	now := time.Now()
	if p.retentionClock != nil {
		now = p.retentionClock()
	}
	if p.retentionClock == nil && !p.retentionClockAnchor.IsZero() {
		if !util.RetentionClockSafe(p.retentionClockAnchor, now, time.Since(p.retentionClockAnchor)) {
			return fmt.Errorf("broker clock step exceeds retention tolerance")
		}
	}
	stamp := now.UnixMilli()
	if stamp <= 0 {
		return fmt.Errorf("invalid broker append clock")
	}
	// Seed from the durable predecessor after restart/leadership change. This
	// also incorporates records received since the last local leader append.
	if tail := p.LEO.Load(); tail > 0 && !p.appendTimeSeeded {
		messages, err := p.dh.ReadMessages(tail-1, 1)
		if err != nil {
			return fmt.Errorf("read append time predecessor: %w", err)
		}
		if len(messages) == 1 && messages[0].AppendTimeMS > p.lastAppendTimeMS {
			p.lastAppendTimeMS = messages[0].AppendTimeMS
		}
		p.appendTimeSeeded = true
	}
	if p.lastAppendTimeMS > stamp+60000 {
		return fmt.Errorf("append time predecessor exceeds broker clock tolerance")
	}
	if stamp < p.lastAppendTimeMS {
		stamp = p.lastAppendTimeMS
	}
	msg.AppendTimeMS = stamp
	p.lastAppendTimeMS = stamp
	return nil
}

func (p *Partition) lockRetentionBoundary() (uint64, func(), error) {
	if !p.visibilityMu.TryRLock() {
		return 0, nil, fmt.Errorf("retention visibility transition in progress")
	}
	if !p.mu.TryRLock() {
		p.visibilityMu.RUnlock()
		return 0, nil, fmt.Errorf("retention partition busy")
	}
	unlock := func() { p.mu.RUnlock(); p.visibilityMu.RUnlock() }
	if p.distributed && p.retentionTimeGate == nil {
		unlock()
		return 0, nil, fmt.Errorf("retention capability state unavailable")
	}
	if p.retentionTimeGate != nil && p.retentionTimeGate() {
		if setter, ok := p.dh.(interface{ EnableRetentionTime() }); ok {
			setter.EnableRetentionTime()
		}
	}
	if p.closed || p.snapshotRecovery || p.bootstrapPending.Load() || p.rollbackPending.Load() {
		unlock()
		return 0, nil, fmt.Errorf("partition retention unavailable")
	}
	if err := p.hwmReadinessErrorLocked(); err != nil {
		unlock()
		return 0, nil, err
	}
	if err := p.RecoveryError(); err != nil {
		unlock()
		return 0, nil, err
	}
	// firstUnresolvedOpenOffset may prune resolved entries from its cache.
	p.txnMarkerMu.Lock()
	floor := firstUnresolvedOpenOffset(min(p.HWM, p.dh.GetFlushedOffset()), p.txnRetentionFloor, p.txnOpenOffsets, p.txnMarkers, p.txnResolver)
	return floor, func() { p.txnMarkerMu.Unlock(); unlock() }, nil
}

func (tm *TopicManager) SetRetentionTimeGate(gate func() bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.retentionTimeGate = gate
	for _, t := range tm.topics {
		t.setRetentionTimeGate(gate)
	}
}

func (t *Topic) setRetentionTimeGate(gate func() bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retentionTimeGate = gate
	for _, p := range t.Partitions {
		p.mu.Lock()
		p.retentionTimeGate = gate
		p.mu.Unlock()
	}
}
