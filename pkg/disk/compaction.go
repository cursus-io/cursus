package disk

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

var errCompactionReadersActive = errors.New("compaction deferred while readers are active")

// CompactionResult describes the work completed by one compaction pass.
type CompactionResult struct {
	SegmentsScanned   int
	SegmentsRewritten int
	RecordsScanned    int
	RecordsRemoved    int
	BytesBefore       int64
	BytesAfter        int64
	SkippedReason     string
}

type compactionFrame struct {
	message types.DiskMessage
	raw     []byte
	size    int
}

// EnforceCompaction rewrites eligible closed segments for compacted topics.
func (d *DiskHandler) EnforceCompaction() (CompactionResult, error) {
	var result CompactionResult
	if !config.HasCleanupPolicy(d.CleanupPolicy(), config.CleanupPolicyCompact) {
		result.SkippedReason = "policy_disabled"
		return result, nil
	}
	if d.distributed {
		return result, fmt.Errorf("log compaction is not supported in distributed mode")
	}

	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	if atomic.LoadInt32(&d.activeReaders) > 0 {
		result.SkippedReason = "active_readers"
		return result, nil
	}

	d.mu.Lock()
	currentSegment := d.CurrentSegment
	segmentSnapshot := append([]uint64(nil), d.segments...)
	d.mu.Unlock()

	closedSegments := make([]uint64, 0, len(segmentSnapshot))
	for _, segment := range segmentSnapshot {
		if segment != currentSegment {
			closedSegments = append(closedSegments, segment)
		}
	}
	sort.Slice(closedSegments, func(i, j int) bool { return closedSegments[i] < closedSegments[j] })
	if len(closedSegments) == 0 {
		result.SkippedReason = "no_closed_segments"
		return result, nil
	}

	segmentAllowsGaps := make(map[uint64]bool, len(closedSegments))
	for _, segment := range closedSegments {
		info, err := os.Stat(d.GetSegmentPath(segment))
		if err != nil {
			return result, fmt.Errorf("stat segment %d: %w", segment, err)
		}
		segmentAllowsGaps[segment] = d.segmentAllowsOffsetGaps(segment, info.Size())
	}

	latestKeyOffset := make(map[string]uint64)
	latestProducerOffset := make(map[string]uint64)
	for _, segment := range closedSegments {
		err := scanCompactionSegment(d.GetSegmentPath(segment), segment, false, segmentAllowsGaps[segment], func(frame compactionFrame) error {
			result.RecordsScanned++
			if isOrdinaryKeyedRecord(frame.message) {
				latestKeyOffset[frame.message.Key] = frame.message.Offset
			}
			if frame.message.ProducerID != "" {
				latestProducerOffset[frame.message.ProducerID] = frame.message.Offset
			}
			return nil
		})
		if err != nil {
			return result, fmt.Errorf("scan segment %d: %w", segment, err)
		}
		result.SegmentsScanned++
	}

	var removableBytes int64
	removableBytesBySegment := make(map[uint64]int64, len(closedSegments))
	for _, segment := range closedSegments {
		var segmentRemovableBytes int64
		err := scanCompactionSegment(d.GetSegmentPath(segment), segment, false, segmentAllowsGaps[segment], func(frame compactionFrame) error {
			frameBytes := int64(frame.size)
			result.BytesBefore += frameBytes
			if shouldRemoveCompactionRecord(frame.message, latestKeyOffset, latestProducerOffset) {
				removableBytes += frameBytes
				segmentRemovableBytes += frameBytes
			}
			return nil
		})
		if err != nil {
			return result, fmt.Errorf("evaluate segment %d: %w", segment, err)
		}
		removableBytesBySegment[segment] = segmentRemovableBytes
	}
	if removableBytes == 0 {
		result.BytesAfter = result.BytesBefore
		result.SkippedReason = "no_superseded_records"
		return result, nil
	}

	ratio := d.compactionDirtyRatio()
	if result.BytesBefore == 0 || float64(removableBytes)/float64(result.BytesBefore) < ratio {
		result.BytesAfter = result.BytesBefore
		result.SkippedReason = "dirty_ratio"
		return result, nil
	}

	var removedBytes int64
	for _, segment := range closedSegments {
		if removableBytesBySegment[segment] == 0 {
			continue
		}
		removed, before, after, err := d.compactClosedSegment(segment, segmentAllowsGaps[segment], latestKeyOffset, latestProducerOffset)
		if errors.Is(err, errCompactionReadersActive) {
			result.BytesAfter = result.BytesBefore - removedBytes
			result.SkippedReason = "active_readers"
			return result, nil
		}
		if err != nil {
			return result, fmt.Errorf("compact segment %d: %w", segment, err)
		}
		if removed > 0 {
			result.SegmentsRewritten++
			result.RecordsRemoved += removed
			removedBytes += before - after
		}
	}
	result.BytesAfter = result.BytesBefore - removedBytes
	return result, nil
}

func (d *DiskHandler) compactionDirtyRatio() float64 {
	d.retentionMu.RLock()
	defer d.retentionMu.RUnlock()
	if d.minCleanableDirtyRatio <= 0 || d.minCleanableDirtyRatio >= 1 {
		return 0.5
	}
	return d.minCleanableDirtyRatio
}

func isOrdinaryKeyedRecord(message types.DiskMessage) bool {
	return message.Key != "" && !hasTransactionMetadata(message)
}

func hasTransactionMetadata(message types.DiskMessage) bool {
	return message.TransactionalID != "" ||
		message.TransactionState != "" ||
		message.TransactionMarker != "" ||
		message.ControlBatchType != "" ||
		message.ControlBatchVersion != 0 ||
		message.ControlBatchCoordinatorEpoch != 0 ||
		len(message.ControlBatchKey) != 0 ||
		len(message.ControlBatchValue) != 0
}

func shouldRemoveCompactionRecord(message types.DiskMessage, latestKeyOffset, latestProducerOffset map[string]uint64) bool {
	if !isOrdinaryKeyedRecord(message) || latestKeyOffset[message.Key] == message.Offset {
		return false
	}
	if message.ProducerID != "" && latestProducerOffset[message.ProducerID] == message.Offset {
		return false
	}
	return true
}

func scanCompactionSegment(path string, expectedBase uint64, includeRaw, allowOffsetGaps bool, visit func(compactionFrame) error) error {
	// #nosec G304 -- path is generated from the broker-owned log directory and a numeric segment ID.
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	var position uint64
	var previousOffset uint64
	havePrevious := false
	for {
		var lengthBytes [4]byte
		_, err := io.ReadFull(reader, lengthBytes[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read record length at byte %d: %w", position, err)
		}

		length := binary.BigEndian.Uint32(lengthBytes[:])
		if length == 0 || length > MaxMessageSize {
			return fmt.Errorf("invalid record length %d at byte %d", length, position)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fmt.Errorf("read record payload at byte %d: %w", position, err)
		}
		message, err := util.DeserializeDiskMessage(payload)
		if err != nil {
			return fmt.Errorf("decode record at byte %d: %w", position, err)
		}
		if !havePrevious && !allowOffsetGaps && message.Offset != expectedBase {
			return fmt.Errorf("unexpected first offset %d for segment base %d", message.Offset, expectedBase)
		}
		if havePrevious && message.Offset <= previousOffset {
			return fmt.Errorf("non-increasing offset %d after %d", message.Offset, previousOffset)
		}
		if havePrevious && !allowOffsetGaps && message.Offset != previousOffset+1 {
			return fmt.Errorf("non-contiguous offset %d after %d", message.Offset, previousOffset)
		}

		size := 4 + len(payload)
		var raw []byte
		if includeRaw {
			raw = make([]byte, size)
			copy(raw[:4], lengthBytes[:])
			copy(raw[4:], payload)
		}
		if err := visit(compactionFrame{message: message, raw: raw, size: size}); err != nil {
			return err
		}
		position += uint64(size)
		previousOffset = message.Offset
		havePrevious = true
	}
}

func (d *DiskHandler) compactClosedSegment(segment uint64, allowOffsetGaps bool, latestKeyOffset, latestProducerOffset map[string]uint64) (int, int64, int64, error) {
	logPath := d.GetSegmentPath(segment)
	indexPath := d.GetIndexPath(segment)
	logTemp := logPath + ".compacting"
	indexTemp := indexPath + ".compacting"
	markerTemp := ""
	_ = os.Remove(logTemp)
	_ = os.Remove(indexTemp)

	info, err := os.Stat(logPath)
	if err != nil {
		return 0, 0, 0, err
	}
	// #nosec G304 -- temporary paths are derived from broker-owned segment paths.
	logFile, err := os.OpenFile(logTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, 0, 0, err
	}
	// #nosec G304 -- temporary paths are derived from broker-owned segment paths.
	indexFile, err := os.OpenFile(indexTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = logFile.Close()
		_ = os.Remove(logTemp)
		return 0, 0, 0, err
	}

	cleanupTemps := true
	defer func() {
		_ = logFile.Close()
		_ = indexFile.Close()
		if cleanupTemps {
			_ = os.Remove(logTemp)
			_ = os.Remove(indexTemp)
			if markerTemp != "" {
				_ = os.Remove(markerTemp)
			}
		}
	}()

	var removed int
	var before int64
	var after int64
	var outputPosition uint64
	var lastIndexPosition uint64
	interval := d.indexInterval
	if interval == 0 {
		interval = 4096
	}
	err = scanCompactionSegment(logPath, segment, true, allowOffsetGaps, func(frame compactionFrame) error {
		before += int64(frame.size)
		if shouldRemoveCompactionRecord(frame.message, latestKeyOffset, latestProducerOffset) {
			removed++
			return nil
		}
		if outputPosition-lastIndexPosition >= interval {
			entry := types.IndexEntry{Offset: frame.message.Offset, Position: outputPosition}
			if err := binary.Write(indexFile, binary.BigEndian, entry); err != nil {
				return fmt.Errorf("write compacted index: %w", err)
			}
			lastIndexPosition = outputPosition
		}
		if _, err := logFile.Write(frame.raw); err != nil {
			return fmt.Errorf("write compacted log: %w", err)
		}
		outputPosition += safeIntToUint64(frame.size)
		after += int64(frame.size)
		return nil
	})
	if err != nil {
		return 0, before, after, err
	}
	if removed == 0 {
		return 0, before, before, nil
	}
	if err := logFile.Sync(); err != nil {
		return 0, before, after, err
	}
	if err := indexFile.Sync(); err != nil {
		return 0, before, after, err
	}
	if err := logFile.Close(); err != nil {
		return 0, before, after, err
	}
	if err := indexFile.Close(); err != nil {
		return 0, before, after, err
	}
	if err := os.Chtimes(logTemp, info.ModTime(), info.ModTime()); err != nil {
		return 0, before, after, err
	}

	markerPath := compactionMarkerPath(logPath, after)
	markerTemp = markerPath + ".compacting"
	_ = os.Remove(markerTemp)
	// #nosec G304 -- the marker path is derived from a broker-owned segment path and its size.
	markerFile, err := os.OpenFile(markerTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, before, after, err
	}
	if _, err := markerFile.WriteString(compactionMarkerVersion); err != nil {
		_ = markerFile.Close()
		return 0, before, after, err
	}
	if err := markerFile.Sync(); err != nil {
		_ = markerFile.Close()
		return 0, before, after, err
	}
	if err := markerFile.Close(); err != nil {
		return 0, before, after, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if segment == d.CurrentSegment {
		return 0, before, before, fmt.Errorf("refusing to compact active segment")
	}
	if atomic.LoadInt32(&d.activeReaders) > 0 {
		return 0, before, before, errCompactionReadersActive
	}

	directory := filepath.Dir(logPath)
	if err := replaceCompactedFile(markerTemp, markerPath); err != nil {
		return 0, before, after, err
	}
	if err := syncDirectory(directory); err != nil {
		_ = os.Remove(markerPath)
		return 0, before, after, err
	}

	originalMode := info.Mode().Perm()
	if originalMode == 0 {
		originalMode = 0o644
	}
	_ = os.Chmod(logPath, 0o600)
	_ = os.Chmod(indexPath, 0o600)
	if err := replaceCompactedFile(logTemp, logPath); err != nil {
		_ = os.Chmod(logPath, originalMode)
		_ = os.Remove(markerPath)
		_ = syncDirectory(directory)
		return 0, before, after, err
	}
	d.recordCompactedSegment(segment, after)
	if err := replaceCompactedFile(indexTemp, indexPath); err != nil {
		_ = os.Chmod(logPath, originalMode)
		_ = syncDirectory(directory)
		return 0, before, after, err
	}
	cleanupTemps = false
	_ = os.Chmod(logPath, originalMode)
	_ = os.Chmod(indexPath, originalMode)
	if err := syncDirectory(directory); err != nil {
		return 0, before, after, err
	}
	return removed, before, after, nil
}

const compactionMarkerVersion = "cursus-compaction-v1\n"

func compactionMarkerPath(logPath string, size int64) string {
	return fmt.Sprintf("%s.compacted-%d", logPath, size)
}

func (d *DiskHandler) segmentAllowsOffsetGaps(segment uint64, actualSize int64) bool {
	d.compactionMu.RLock()
	defer d.compactionMu.RUnlock()
	expectedSize, ok := d.compactedSegments[segment]
	return ok && expectedSize == actualSize
}

func (d *DiskHandler) recordCompactedSegment(segment uint64, size int64) {
	d.compactionMu.Lock()
	if d.compactedSegments == nil {
		d.compactedSegments = make(map[uint64]int64)
	}
	d.compactedSegments[segment] = size
	d.compactionMu.Unlock()
}

func (d *DiskHandler) forgetCompactedSegment(segment uint64) {
	d.compactionMu.Lock()
	delete(d.compactedSegments, segment)
	d.compactionMu.Unlock()
}

func loadCompactionMarkers(base string) (map[uint64]int64, error) {
	compacted := make(map[uint64]int64)
	paths, err := filepath.Glob(base + "_segment_*.log.compacted-*")
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return compacted, nil
	}

	removed := false
	prefix := base + "_segment_"
	const delimiter = ".log.compacted-"
	for _, path := range paths {
		remainder := strings.TrimPrefix(path, prefix)
		separator := strings.Index(remainder, delimiter)
		if remainder == path || separator <= 0 {
			_ = os.Chmod(path, 0o600)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return nil, err
			}
			removed = true
			continue
		}

		segment, segmentErr := strconv.ParseUint(remainder[:separator], 10, 64)
		expectedSize, sizeErr := strconv.ParseInt(remainder[separator+len(delimiter):], 10, 64)
		logPath := path[:strings.LastIndex(path, ".compacted-")]
		info, statErr := os.Stat(logPath)
		if segmentErr != nil || sizeErr != nil || expectedSize < 0 || statErr != nil || info.Size() != expectedSize {
			if statErr != nil && !os.IsNotExist(statErr) {
				return nil, statErr
			}
			_ = os.Chmod(path, 0o600)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return nil, err
			}
			removed = true
			continue
		}

		// #nosec G304 -- marker paths are discovered only under the broker-owned log directory.
		marker, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if string(marker) != compactionMarkerVersion {
			return nil, fmt.Errorf("invalid compaction marker %s", path)
		}
		compacted[segment] = expectedSize
	}
	if removed {
		if err := syncDirectory(filepath.Dir(base)); err != nil {
			return nil, err
		}
	}
	return compacted, nil
}

func cleanupCompactionMarkersForLog(logPath string, keepSize int64) error {
	paths, err := filepath.Glob(logPath + ".compacted-*")
	if err != nil {
		return err
	}
	keepPath := ""
	if keepSize >= 0 {
		keepPath = compactionMarkerPath(logPath, keepSize)
	}
	for _, path := range paths {
		if path == keepPath {
			continue
		}
		_ = os.Chmod(path, 0o600)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
func cleanupCompactionTemps(base string) error {
	paths, err := filepath.Glob(base + "_segment_*.compacting")
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}
	for _, path := range paths {
		_ = os.Chmod(path, 0o600)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove compaction temporary file %s: %w", path, err)
		}
	}
	return syncDirectory(filepath.Dir(base))
}
