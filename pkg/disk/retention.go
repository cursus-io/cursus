package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/util"
)

type ReadSession struct {
	File    *os.File
	handler *DiskHandler
}

func (s *ReadSession) Close() error {
	atomic.AddInt32(&s.handler.activeReaders, -1)
	return s.File.Close()
}

func (d *DiskHandler) OpenForRead(offset uint64) (*ReadSession, error) {
	path, _, err := d.findSegmentForOffset(offset)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&d.activeReaders, 1)

	return &ReadSession{
		File:    f,
		handler: d,
	}, nil
}

func (d *DiskHandler) EnforceRetention(cfg *config.Config) {
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
	retentionDuration := time.Duration(cfg.RetentionHours) * time.Hour

	for i := 0; i < len(metas)-1; i++ {
		meta := metas[i]

		if meta.info.Mode().Perm() != 0444 {
			_ = os.Chmod(meta.path, 0444)
			indexPath := strings.TrimSuffix(meta.path, ".log") + ".index"
			_ = os.Chmod(indexPath, 0444)
			util.Debug("Segment %s secured (read-only)", filepath.Base(meta.path))
		}

		isExpired := cfg.RetentionHours > 0 && now.Sub(meta.info.ModTime()) > retentionDuration
		isOverCapacity := cfg.RetentionBytes > 0 && totalSize > cfg.RetentionBytes
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
	fileName := filepath.Base(logPath)
	parts := strings.Split(fileName, "_")
	if len(parts) < 4 {
		return fmt.Errorf("invalid segment filename: %s", fileName)
	}

	numStr := strings.TrimSuffix(parts[3], ".log")
	segmentOffset, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse offset from filename %s: %w", fileName, err)
	}

	deletedLogPath := logPath + ".deleted"
	indexPath := logPath[:len(logPath)-4] + ".index"
	deletedIndexPath := indexPath + ".deleted"

	if err := os.Rename(logPath, deletedLogPath); err != nil {
		return err
	}

	if _, err := os.Stat(indexPath); err == nil {
		if err := os.Rename(indexPath, deletedIndexPath); err != nil {
			return fmt.Errorf("rename index failed: %w", err)
		}
	}

	for i, s := range d.segments {
		if s == segmentOffset {
			d.segments = append(d.segments[:i], d.segments[i+1:]...)
			break
		}
	}
	return nil
}

func (d *DiskHandler) retentionLoop(cfg *config.Config) {
	interval := cfg.RetentionCheckIntervalMS
	if interval <= 0 {
		interval = 300000
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cfg.CleanupPolicy == "delete" {
				d.EnforceRetention(cfg)
			}
		case <-d.done:
			return
		}
	}
}
