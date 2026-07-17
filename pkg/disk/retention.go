package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/util"
)

type retentionLimits struct {
	hours int
	bytes int64
}

type ReadSession struct {
	File      *os.File
	handler   *DiskHandler
	closeOnce sync.Once
	closeErr  error
}

func (s *ReadSession) Close() error {
	s.closeOnce.Do(func() {
		atomic.AddInt32(&s.handler.activeReaders, -1)
		s.closeErr = s.File.Close()
	})
	return s.closeErr
}

func (d *DiskHandler) OpenForRead(offset uint64) (*ReadSession, error) {
	atomic.AddInt32(&d.activeReaders, 1)
	path, _, err := d.findSegmentForOffset(offset)
	if err != nil {
		atomic.AddInt32(&d.activeReaders, -1)
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		atomic.AddInt32(&d.activeReaders, -1)
		return nil, err
	}

	return &ReadSession{
		File:    f,
		handler: d,
	}, nil
}

// SetRetentionPolicy applies topic-level limits. Zero means inherit the broker default.
func (d *DiskHandler) SetRetentionPolicy(hours int, bytes int64) {
	d.retentionMu.Lock()
	defer d.retentionMu.Unlock()

	if hours == 0 {
		hours = d.retentionDefaults.hours
	}
	if bytes == 0 {
		bytes = d.retentionDefaults.bytes
	}
	d.retentionPolicy = retentionLimits{hours: hours, bytes: bytes}
	d.retentionConfigured = true
}

// SetCleanupPolicy changes the maintenance policy for this partition.
func (d *DiskHandler) SetCleanupPolicy(policy string) {
	normalized, ok := config.NormalizeCleanupPolicy(policy)
	if !ok {
		normalized = config.CleanupPolicyDelete
	}
	d.retentionMu.Lock()
	d.cleanupPolicy = normalized
	d.retentionMu.Unlock()
}

// SetStoragePolicy atomically applies topic cleanup and retention settings.
func (d *DiskHandler) SetStoragePolicy(cleanupPolicy string, hours int, bytes int64) {
	normalized, ok := config.NormalizeCleanupPolicy(cleanupPolicy)
	if !ok {
		normalized = config.CleanupPolicyDelete
	}
	d.retentionMu.Lock()
	defer d.retentionMu.Unlock()
	if hours == 0 {
		hours = d.retentionDefaults.hours
	}
	if bytes == 0 {
		bytes = d.retentionDefaults.bytes
	}
	d.retentionPolicy = retentionLimits{hours: hours, bytes: bytes}
	d.retentionConfigured = true
	d.cleanupPolicy = normalized
}

func (d *DiskHandler) CleanupPolicy() string {
	d.retentionMu.RLock()
	defer d.retentionMu.RUnlock()
	if d.cleanupPolicy == "" {
		return config.CleanupPolicyDelete
	}
	return d.cleanupPolicy
}

func (d *DiskHandler) RetentionPolicy() (int, int64) {
	d.retentionMu.RLock()
	defer d.retentionMu.RUnlock()
	return d.retentionPolicy.hours, d.retentionPolicy.bytes
}

func (d *DiskHandler) effectiveRetention(cfg *config.Config) retentionLimits {
	d.retentionMu.RLock()
	defer d.retentionMu.RUnlock()
	if d.retentionConfigured {
		return d.retentionPolicy
	}
	if cfg == nil {
		return retentionLimits{}
	}
	return retentionLimits{hours: cfg.RetentionHours, bytes: cfg.RetentionBytes}
}

func (d *DiskHandler) EnforceRetention(cfg *config.Config) {
	if !config.HasCleanupPolicy(d.CleanupPolicy(), config.CleanupPolicyDelete) {
		return
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	if atomic.LoadInt32(&d.activeReaders) > 0 {
		util.Debug("Retention skipped: %d active readers", atomic.LoadInt32(&d.activeReaders))
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if atomic.LoadInt32(&d.activeReaders) > 0 {
		return
	}

	pattern := d.BaseName + "_segment_*.log"
	files, err := filepath.Glob(pattern)
	if err != nil {
		util.Error("retention glob failed: %v", err)
	}
	if len(files) <= 1 {
		return
	}
	sort.Strings(files)

	type fileMeta struct {
		path string
		info os.FileInfo
	}
	var metas []fileMeta
	var totalSize int64
	for _, f := range files {
		if info, err := os.Stat(f); err == nil {
			metas = append(metas, fileMeta{f, info})
			totalSize += info.Size()
		}
	}

	now := time.Now()
	limits := d.effectiveRetention(cfg)
	retentionDuration := time.Duration(limits.hours) * time.Hour

	for i := 0; i < len(metas)-1; i++ {
		meta := metas[i]

		if meta.info.Mode().Perm() != 0444 {
			_ = os.Chmod(meta.path, 0444)
			indexPath := strings.TrimSuffix(meta.path, ".log") + ".index"
			_ = os.Chmod(indexPath, 0444)
			util.Debug("Segment %s secured (read-only)", filepath.Base(meta.path))
		}

		isExpired := limits.hours > 0 && now.Sub(meta.info.ModTime()) > retentionDuration
		isOverCapacity := limits.bytes > 0 && totalSize > limits.bytes
		if isExpired || isOverCapacity {
			fileSize := meta.info.Size()
			if err := d.markAsDeleted(meta.path); err == nil {
				totalSize -= fileSize
			} else {
				util.Debug("retention failed to delete %s: %v", meta.path, err)
				break
			}
		}
	}
}

func (d *DiskHandler) markAsDeleted(logPath string) error {
	prefix := d.BaseName + "_segment_"
	if !strings.HasPrefix(logPath, prefix) || !strings.HasSuffix(logPath, ".log") {
		return fmt.Errorf("invalid segment path: %s", logPath)
	}
	numStr := strings.TrimSuffix(strings.TrimPrefix(logPath, prefix), ".log")
	segmentOffset, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse offset from filename %s: %w", filepath.Base(logPath), err)
	}

	deletedLogPath := logPath + ".deleted"
	indexPath := logPath[:len(logPath)-4] + ".index"
	deletedIndexPath := indexPath + ".deleted"
	renamedIndex := false

	if _, err := os.Stat(indexPath); err == nil {
		_ = os.Chmod(indexPath, 0o600)
		if err := os.Rename(indexPath, deletedIndexPath); err != nil {
			return fmt.Errorf("rename index tombstone: %w", err)
		}
		renamedIndex = true
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat index for deletion: %w", err)
	}

	_ = os.Chmod(logPath, 0o600)
	if err := os.Rename(logPath, deletedLogPath); err != nil {
		if renamedIndex {
			_ = os.Rename(deletedIndexPath, indexPath)
		}
		return err
	}
	if err := cleanupCompactionMarkersForLog(logPath, -1); err != nil {
		util.Warn("failed to remove compaction marker for %s: %v", logPath, err)
	}
	d.forgetCompactedSegment(segmentOffset)

	for i, s := range d.segments {
		if s == segmentOffset {
			d.segments = append(d.segments[:i], d.segments[i+1:]...)
			break
		}
	}

	directory := filepath.Dir(logPath)
	syncErr := syncDirectory(directory)
	for _, tombstone := range []string{deletedLogPath, deletedIndexPath} {
		_ = os.Chmod(tombstone, 0o600)
		if err := os.Remove(tombstone); err != nil && !os.IsNotExist(err) {
			util.Warn("failed to purge segment tombstone %s: %v", tombstone, err)
		}
	}
	if err := syncDirectory(directory); err != nil && syncErr == nil {
		syncErr = err
	}
	return syncErr
}

func cleanupDeletedSegments(base string) error {
	paths, err := filepath.Glob(base + "_segment_*.deleted")
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}
	for _, path := range paths {
		_ = os.Chmod(path, 0o600)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove tombstone %s: %w", path, err)
		}
	}
	return syncDirectory(filepath.Dir(base))
}

func (d *DiskHandler) retentionLoop(cfg *config.Config) {
	retentionInterval := cfg.RetentionCheckIntervalMS
	if retentionInterval <= 0 {
		retentionInterval = 300000
	}
	compactionInterval := cfg.CompactionCheckIntervalMS
	if compactionInterval <= 0 {
		compactionInterval = 300000
	}

	retentionTicker := time.NewTicker(time.Duration(retentionInterval) * time.Millisecond)
	compactionTicker := time.NewTicker(time.Duration(compactionInterval) * time.Millisecond)
	defer retentionTicker.Stop()
	defer compactionTicker.Stop()

	for {
		select {
		case <-retentionTicker.C:
			d.EnforceRetention(cfg)
		case <-compactionTicker.C:
			if _, err := d.EnforceCompaction(); err != nil {
				util.Error("log compaction failed for %s: %v", d.BaseName, err)
			}
		case <-d.done:
			return
		}
	}
}
