package disk

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/util"
)

type segmentTimeSummary struct {
	info      os.FileInfo
	maxMS     int64
	unknown   bool
	protected bool
	err       error
}

// RetentionTimeWork counts logical bytes returned by file reads, not physical
// device I/O. Summaries are memory-only: rebuilding writes zero bytes.
type RetentionTimeWork struct {
	ReadBytes, Records, Rebuilds, CacheHits uint64
	UnknownSegments, InvalidSegments        uint64
}

func (d *DiskHandler) RetentionTimeWorkSnapshot() RetentionTimeWork {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.retentionTimeWork
}

// SetRetentionGuard supplies a stable exclusive deletion boundary and an
// unlock function. It runs BEFORE storage locks, preserving partition->disk
// lock order, and holds the partition visibility guard through deletion.
func (d *DiskHandler) SetRetentionGuard(guard func() (uint64, func(), error)) {
	d.retentionGuardMu.Lock()
	defer d.retentionGuardMu.Unlock()
	d.retentionGuard = guard
}

// EnableRetentionTime is irreversible for this handler. Cluster activation is
// durable in Raft; every process reapplies it before a retention decision.
func (d *DiskHandler) EnableRetentionTime() {
	d.mu.Lock()
	d.retentionTimeEnabled = true
	d.mu.Unlock()
}

func (d *DiskHandler) timeNow() time.Time {
	if d.retentionClock != nil {
		return d.retentionClock()
	}
	return time.Now()
}

func (d *DiskHandler) segmentTimeLocked(path string, info os.FileInfo) segmentTimeSummary {
	if old, ok := d.retentionTimes[path]; ok && os.SameFile(old.info, info) && old.info.Size() == info.Size() && old.info.ModTime() == info.ModTime() {
		d.retentionTimeWork.CacheHits++
		metrics.RecordRecoveryWork(metrics.RecoveryRetentionTime, metrics.RecoveryCacheHits, 1)
		return old
	}
	result := segmentTimeSummary{info: info}
	d.retentionTimeWork.Rebuilds++
	metrics.RecordRecoveryWork(metrics.RecoveryRetentionTime, metrics.RecoveryCacheMisses, 1)
	result.err = scanCompactionSegmentObserved(context.Background(), path, 0, false, true, func(frame compactionFrame) error {
		d.retentionTimeWork.Records++
		metrics.RecordRecoveryWork(metrics.RecoveryRetentionTime, metrics.RecoveryDecodedFrames, 1)
		if frame.message.AppendTimeMS == 0 {
			result.unknown = true
		}
		// Event histories require a snapshot-aware deletion proof. Until one is
		// supplied, neither age nor disk pressure can destroy their history.
		if frame.message.EventType != "" || frame.message.AggregateVersion != 0 || frame.message.EventID != "" {
			result.protected = true
		}
		if frame.message.AppendTimeMS > result.maxMS {
			result.maxMS = frame.message.AppendTimeMS
		}
		return nil
	}, func(n int) {
		d.retentionTimeWork.ReadBytes += uint64(n)
		metrics.RecordRecoveryBytes(metrics.RecoveryRetentionTime, metrics.RecoveryReadBytes, int64(n))
	})
	metrics.RecordRecoveryAttempt(metrics.RecoveryRetentionTime, result.err)
	if result.unknown {
		d.retentionTimeWork.UnknownSegments++
	}
	if result.err != nil {
		d.retentionTimeWork.InvalidSegments++
	}
	if d.retentionTimes == nil {
		d.retentionTimes = make(map[string]segmentTimeSummary)
	}
	d.retentionTimes[path] = result
	return result
}

// Timed records always keep their contract, including when activation is
// disabled after restart. Unknown legacy time is held under the new contract.
func (d *DiskHandler) timeExpiredLocked(path string, info os.FileInfo, now time.Time, hours int) bool {
	if hours <= 0 {
		return false
	}
	if d.retentionClock == nil && !util.RetentionClockSafe(d.retentionClockAnchor, now, time.Since(d.retentionClockAnchor)) {
		metrics.RecordRetentionHold("clock_step")
		return false
	}
	s := d.segmentTimeLocked(path, info)
	if s.err != nil {
		metrics.RecordRetentionHold("corrupt_data")
		return false
	}
	if s.unknown || s.maxMS == 0 {
		if d.retentionTimeEnabled || s.maxMS != 0 {
			metrics.RecordRetentionHold("unknown_time")
		}
		return !d.retentionTimeEnabled && s.maxMS == 0 && now.Sub(info.ModTime()) > time.Duration(hours)*time.Hour
	}
	if s.maxMS > now.UnixMilli() {
		metrics.RecordRetentionHold("future_time")
		return false
	}
	// Avoid duration overflow for untrusted timestamp extremes or huge policies.
	return (now.UnixMilli()-s.maxMS)/3600000 > int64(hours) ||
		(now.UnixMilli()-s.maxMS)/3600000 == int64(hours) && (now.UnixMilli()-s.maxMS)%3600000 > 0
}

func (d *DiskHandler) retentionBoundary() (uint64, func(), error) {
	d.retentionGuardMu.RLock()
	guard := d.retentionGuard
	d.retentionGuardMu.RUnlock()
	if guard != nil {
		return guard()
	}
	d.mu.Lock()
	requiresGuard := d.distributed || d.retentionTimeEnabled
	d.mu.Unlock()
	if requiresGuard {
		return 0, nil, fmt.Errorf("retention requires committed boundary")
	}
	return ^uint64(0), func() {}, nil
}
