package disk

import (
	"errors"
	"fmt"
	"os"
)

// RuntimeSnapshot is an aggregate view of open broker storage handlers.
type RuntimeSnapshot struct {
	Handlers              int
	Segments              int
	Bytes                 int64
	PendingWrites         int
	ActiveReaders         int
	StatFailures          int
	SegmentCacheEntries   int
	SegmentCacheHits      uint64
	SegmentCacheMisses    uint64
	SegmentCacheEvictions uint64
	FilesystemFreeBytes   uint64
	FilesystemTotalBytes  uint64
	FilesystemHeadroomOK  bool
}

// RuntimeSnapshot samples storage state and may block on filesystem calls or
// locks shared with fsync. Call it only from a bounded background observer.
func (dm *DiskManager) RuntimeSnapshot() RuntimeSnapshot {
	return dm.runtimeSnapshot(os.Stat)
}

func (dm *DiskManager) runtimeSnapshot(stat func(string) (os.FileInfo, error)) RuntimeSnapshot {
	if dm == nil {
		return RuntimeSnapshot{StatFailures: 1}
	}

	dm.mu.Lock()
	handlers := make([]*DiskHandler, 0, len(dm.handlers))
	for _, handler := range dm.handlers {
		handlers = append(handlers, handler)
	}
	dm.mu.Unlock()

	snapshot := RuntimeSnapshot{Handlers: len(handlers)}
	if space, err := dm.headroom.snapshot(true); err == nil || errors.Is(err, errInsufficientHeadroom) {
		// Low headroom is a measured unhealthy state, not a failed measurement.
		// Keep its capacity and false readiness visible to the disk-space alert.
		snapshot.FilesystemFreeBytes = space.Free
		snapshot.FilesystemTotalBytes = space.Total
		snapshot.FilesystemHeadroomOK = space.Ready
	} else {
		snapshot.StatFailures++
	}
	for _, handler := range handlers {
		if handler == nil {
			continue
		}

		files, missing := handler.runtimeFiles(stat)
		if missing {
			// Retention/truncation may change membership or recreate the index
			// after we copy the segment list. Reconcile once with the mutation
			// lock and resample the entire handler to avoid mixed byte counts.
			// Persistent absence remains a failure; there is no retry loop.
			files, _ = handler.runtimeFiles(stat)
		}
		snapshot.Segments += files.Segments
		snapshot.Bytes += files.Bytes
		snapshot.StatFailures += files.StatFailures
		snapshot.PendingWrites += files.PendingWrites
		snapshot.ActiveReaders += int(handler.GetActiveReaders())
		cache := handler.segmentReaders.stats()
		snapshot.SegmentCacheEntries += cache.Entries
		snapshot.SegmentCacheHits += cache.Hits
		snapshot.SegmentCacheMisses += cache.Misses
		snapshot.SegmentCacheEvictions += cache.Evictions
	}

	return snapshot
}

func (d *DiskHandler) runtimeFiles(stat func(string) (os.FileInfo, error)) (RuntimeSnapshot, bool) {
	d.mu.Lock()
	segments := append([]uint64(nil), d.segments...)
	segments = append(segments, d.CurrentSegment)
	result := RuntimeSnapshot{PendingWrites: len(d.writeCh)}
	d.mu.Unlock()

	missing := false
	seen := make(map[uint64]struct{}, len(segments))
	for _, base := range segments {
		if _, exists := seen[base]; exists {
			continue
		}
		seen[base] = struct{}{}
		result.Segments++
		for _, path := range []string{d.GetSegmentPath(base), d.GetIndexPath(base)} {
			info, err := stat(path)
			if err != nil {
				missing = missing || os.IsNotExist(err)
				result.StatFailures++
				continue
			}
			result.Bytes += info.Size()
		}
	}
	return result, missing
}

// Ready verifies that the configured log directory and active handlers are available.
// It performs synchronous I/O and must not run on a health HTTP request goroutine.
func (dm *DiskManager) Ready() error {
	if dm == nil || dm.cfg == nil {
		return fmt.Errorf("disk manager unavailable")
	}
	if dm.cfg.LogDir == "" {
		return fmt.Errorf("log directory is not configured")
	}
	info, err := os.Stat(dm.cfg.LogDir)
	if err != nil {
		return fmt.Errorf("inspect log directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("log path is not a directory")
	}
	if _, err := dm.headroom.snapshot(true); err != nil {
		return fmt.Errorf("storage admission unavailable: %w", err)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	for key, handler := range dm.handlers {
		if handler == nil {
			return fmt.Errorf("storage handler %s is unavailable", key)
		}
		if err := handler.writeAvailabilityError(); err != nil {
			return fmt.Errorf("storage handler %s is not writable: %w", key, err)
		}
		select {
		case <-handler.done:
			return fmt.Errorf("storage handler %s is closed", key)
		default:
		}
	}
	return nil
}
