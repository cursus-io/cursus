package disk

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const compactedRangeTransactionVersion = 1

type compactedRangeTransaction struct {
	Version       int    `json:"version"`
	StartOffset   uint64 `json:"start_offset"`
	EndOffset     uint64 `json:"end_offset"`
	LogSize       int64  `json:"log_size"`
	IndexSize     int64  `json:"index_size"`
	LogChecksum   string `json:"log_checksum"`
	IndexChecksum string `json:"index_checksum"`
}

// WriteCompactedReplicaRange appends a committed logical range from a
// compacted leader. Missing physical offsets are represented by the existing
// compacted-segment marker, never by synthetic consumer-visible records.
// A durable pending manifest makes the multi-file installation restart-safe.
func (d *DiskHandler) WriteCompactedReplicaRange(startOffset, endOffset uint64, batch []types.DiskMessage) error {
	if err := validateCompactedReplicaRange(startOffset, endOffset, batch); err != nil {
		return err
	}

	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}
	if atomic.LoadInt32(&d.activeReaders) > 0 {
		return errCompactionReadersActive
	}
	if current := atomic.LoadUint64(&d.AbsoluteOffset); current != startOffset {
		return fmt.Errorf("compacted replica range starts at %d but local tail is %d", startOffset, current)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if d.CurrentSegment != startOffset || d.CurrentOffset != 0 {
		if err := d.rotateSegment(startOffset); err != nil {
			return fmt.Errorf("roll compacted replica range start: %w", err)
		}
	}

	transaction, err := d.stageCompactedReplicaRange(startOffset, endOffset, batch)
	if err != nil {
		return fmt.Errorf("stage compacted replica range: %w", err)
	}
	if err := d.closeActiveSegmentForCompactedRange(); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("close compacted replica range target: %w", err))
	}
	if err := completeCompactedRangeTransaction(d.BaseName, transaction); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("commit compacted replica range: %w", err))
	}

	d.CurrentSegment = endOffset
	d.CurrentOffset = 0
	d.lastIndexPosition = 0
	d.indexBytesWritten = 0
	d.segmentCreatedAt = time.Now()
	d.segments = appendSegmentIfMissing(d.segments, endOffset)
	if err := d.openSegment(); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("open compacted replica range tail: %w", err))
	}
	if err := d.openIndexFiles(); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("open compacted replica range index: %w", err))
	}
	d.recordCompactedSegment(startOffset, transaction.LogSize)
	atomic.StoreUint64(&d.AbsoluteOffset, endOffset)
	atomic.StoreUint64(&d.FlushedOffset, endOffset)
	return nil
}

func validateCompactedReplicaRange(startOffset, endOffset uint64, batch []types.DiskMessage) error {
	if endOffset <= startOffset {
		return fmt.Errorf("invalid compacted replica range [%d,%d)", startOffset, endOffset)
	}
	previous := startOffset
	for i := range batch {
		if batch[i].Offset < startOffset || batch[i].Offset >= endOffset {
			return fmt.Errorf("compacted replica offset %d is outside [%d,%d)", batch[i].Offset, startOffset, endOffset)
		}
		if i > 0 && batch[i].Offset <= previous {
			return fmt.Errorf("compacted replica offsets are not strictly increasing at %d", batch[i].Offset)
		}
		previous = batch[i].Offset
	}
	return nil
}

func (d *DiskHandler) stageCompactedReplicaRange(startOffset, endOffset uint64, batch []types.DiskMessage) (compactedRangeTransaction, error) {
	transaction := compactedRangeTransaction{
		Version: compactedRangeTransactionVersion, StartOffset: startOffset, EndOffset: endOffset,
	}
	logPath := d.GetSegmentPath(startOffset)
	indexPath := d.GetIndexPath(startOffset)
	logTemp := compactedRangeTempPath(logPath)
	indexTemp := compactedRangeTempPath(indexPath)
	pendingPath := compactedRangePendingPath(d.BaseName)
	pendingTemp := compactedRangePendingTempPath(d.BaseName)
	for _, path := range []string{logTemp, indexTemp, pendingTemp} {
		_ = os.Remove(path)
	}
	if _, err := os.Stat(pendingPath); err == nil {
		return transaction, fmt.Errorf("pending compacted replica range already exists")
	} else if !os.IsNotExist(err) {
		return transaction, err
	}

	// #nosec G304 -- temporary paths are derived from broker-owned segment paths.
	logFile, err := os.OpenFile(logTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return transaction, err
	}
	// #nosec G304 -- temporary paths are derived from broker-owned segment paths.
	indexFile, err := os.OpenFile(indexTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = logFile.Close()
		_ = os.Remove(logTemp)
		return transaction, err
	}
	keepStaged := false
	defer func() {
		_ = logFile.Close()
		_ = indexFile.Close()
		if !keepStaged {
			_ = os.Remove(logTemp)
			_ = os.Remove(indexTemp)
			_ = os.Remove(pendingTemp)
		}
	}()

	logDigest := sha256.New()
	indexDigest := sha256.New()
	logWriter := io.MultiWriter(logFile, logDigest)
	indexWriter := io.MultiWriter(indexFile, indexDigest)
	interval := d.indexInterval
	if interval == 0 {
		interval = 4096
	}
	var outputPosition uint64
	var lastIndexPosition uint64
	for i := range batch {
		serialized, err := util.SerializeDiskMessage(batch[i])
		if err != nil {
			return transaction, fmt.Errorf("serialize record %d: %w", i, err)
		}
		if err := validateSerializedDiskMessageSize(serialized); err != nil {
			return transaction, fmt.Errorf("record %d: %w", i, err)
		}
		if outputPosition-lastIndexPosition >= interval {
			entry := types.IndexEntry{Offset: batch[i].Offset, Position: outputPosition}
			if err := binary.Write(indexWriter, binary.BigEndian, entry); err != nil {
				return transaction, fmt.Errorf("write compacted range index: %w", err)
			}
			lastIndexPosition = outputPosition
		}
		var lengthBytes [4]byte
		length, ok := util.SafeIntToUint32(len(serialized))
		if !ok {
			return transaction, fmt.Errorf("record %d is too large", i)
		}
		binary.BigEndian.PutUint32(lengthBytes[:], length)
		if _, err := logWriter.Write(lengthBytes[:]); err != nil {
			return transaction, err
		}
		if _, err := logWriter.Write(serialized); err != nil {
			return transaction, err
		}
		outputPosition += uint64(len(lengthBytes) + len(serialized))
	}
	if err := d.syncFile(logFile); err != nil {
		return transaction, err
	}
	if err := indexFile.Sync(); err != nil {
		return transaction, err
	}
	if err := logFile.Close(); err != nil {
		return transaction, err
	}
	if err := indexFile.Close(); err != nil {
		return transaction, err
	}
	if outputPosition > math.MaxInt64 {
		return transaction, fmt.Errorf("compacted range size %d exceeds int64", outputPosition)
	}
	transaction.LogSize = int64(outputPosition) // #nosec G115 -- outputPosition is bounded above before narrowing.
	indexInfo, err := os.Stat(indexTemp)
	if err != nil {
		return transaction, err
	}
	transaction.IndexSize = indexInfo.Size()
	transaction.LogChecksum = hex.EncodeToString(logDigest.Sum(nil))
	transaction.IndexChecksum = hex.EncodeToString(indexDigest.Sum(nil))
	if err := writeCompactedRangePending(pendingTemp, pendingPath, transaction); err != nil {
		return transaction, err
	}
	keepStaged = true
	return transaction, nil
}

func (d *DiskHandler) closeActiveSegmentForCompactedRange() error {
	var errs []error
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			errs = append(errs, err)
		}
		d.writer = nil
	}
	if d.file != nil {
		if err := d.syncFile(d.file); err != nil {
			errs = append(errs, err)
		}
		if err := d.file.Close(); err != nil {
			errs = append(errs, err)
		}
		d.file = nil
	}
	d.indexMu.Lock()
	if err := d.closeIndexFiles(); err != nil {
		errs = append(errs, err)
	}
	d.indexMu.Unlock()
	if len(errs) != 0 {
		return fmt.Errorf("close active segment: %v", errs)
	}
	return nil
}

func writeCompactedRangePending(tempPath, pendingPath string, transaction compactedRangeTransaction) error {
	// #nosec G304 -- paths are derived from the broker-owned partition base.
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := json.NewEncoder(file).Encode(transaction); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := replaceCompactedFile(tempPath, pendingPath); err != nil {
		return err
	}
	removeTemp = false
	return syncDirectory(filepath.Dir(pendingPath))
}

func recoverPendingCompactedRange(base string) error {
	pendingPath := compactedRangePendingPath(base)
	// #nosec G304 -- pendingPath is derived from the broker-owned partition base.
	data, err := os.ReadFile(pendingPath)
	if os.IsNotExist(err) {
		_ = os.Remove(compactedRangePendingTempPath(base))
		return nil
	}
	if err != nil {
		return err
	}
	var transaction compactedRangeTransaction
	if err := json.Unmarshal(data, &transaction); err != nil {
		return fmt.Errorf("decode pending compacted range: %w", err)
	}
	return completeCompactedRangeTransaction(base, transaction)
}

func completeCompactedRangeTransaction(base string, transaction compactedRangeTransaction) error {
	if transaction.Version != compactedRangeTransactionVersion || transaction.EndOffset <= transaction.StartOffset ||
		transaction.LogSize < 0 || transaction.IndexSize < 0 || transaction.LogChecksum == "" || transaction.IndexChecksum == "" {
		return fmt.Errorf("invalid pending compacted range")
	}
	tempHandler := &DiskHandler{BaseName: base}
	logPath := tempHandler.GetSegmentPath(transaction.StartOffset)
	indexPath := tempHandler.GetIndexPath(transaction.StartOffset)
	if err := installCompactedRangeFile(compactedRangeTempPath(logPath), logPath, transaction.LogSize, transaction.LogChecksum); err != nil {
		return fmt.Errorf("install compacted range log: %w", err)
	}
	if err := installCompactedRangeFile(compactedRangeTempPath(indexPath), indexPath, transaction.IndexSize, transaction.IndexChecksum); err != nil {
		return fmt.Errorf("install compacted range index: %w", err)
	}
	if err := writeCompactionMarker(logPath, transaction.LogSize); err != nil {
		return fmt.Errorf("install compacted range marker: %w", err)
	}
	if err := ensureEmptyCompactedRangeTail(tempHandler.GetSegmentPath(transaction.EndOffset)); err != nil {
		return fmt.Errorf("create compacted range tail: %w", err)
	}
	if err := ensureEmptyCompactedRangeTail(tempHandler.GetIndexPath(transaction.EndOffset)); err != nil {
		return fmt.Errorf("create compacted range tail index: %w", err)
	}
	directory := filepath.Dir(base)
	if err := syncDirectory(directory); err != nil {
		return err
	}
	// The durable pending manifest is the recovery authority and is removed last.
	for _, path := range []string{compactedRangePendingTempPath(base), compactedRangePendingPath(base)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncDirectory(directory)
}

func installCompactedRangeFile(tempPath, destination string, size int64, checksum string) error {
	if _, err := os.Stat(tempPath); err == nil {
		if err := validateCompactedRangeFile(tempPath, size, checksum); err != nil {
			return err
		}
		return replaceCompactedFile(tempPath, destination)
	} else if !os.IsNotExist(err) {
		return err
	}
	return validateCompactedRangeFile(destination, size, checksum)
}

func validateCompactedRangeFile(path string, size int64, checksum string) error {
	// #nosec G304 -- path is derived from the broker-owned partition base.
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("file size %d does not match pending size %d", info.Size(), size)
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return err
	}
	if actual := hex.EncodeToString(digest.Sum(nil)); actual != checksum {
		return fmt.Errorf("file checksum does not match pending checksum")
	}
	return nil
}

func ensureEmptyCompactedRangeTail(path string) error {
	// #nosec G304 -- path is derived from the broker-owned partition base.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if os.IsExist(err) {
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		if info.Size() != 0 {
			return fmt.Errorf("existing compacted range tail is not empty")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func appendSegmentIfMissing(segments []uint64, segment uint64) []uint64 {
	for _, existing := range segments {
		if existing == segment {
			return segments
		}
	}
	segments = append(segments, segment)
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	return segments
}

func compactedRangeTempPath(path string) string {
	return path + ".range.compacting"
}

func compactedRangePendingPath(base string) string {
	return base + "_compacted_range.pending"
}

func compactedRangePendingTempPath(base string) string {
	return compactedRangePendingPath(base) + ".compacting"
}

func writeCompactionMarker(logPath string, size int64) error {
	if err := cleanupCompactionMarkersForLog(logPath, -1); err != nil {
		return err
	}
	markerPath := compactionMarkerPath(logPath, size)
	tempPath := markerPath + ".compacting"
	_ = os.Remove(tempPath)
	// #nosec G304 -- paths are derived from the broker-owned log segment.
	marker, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	removeTemp := true
	defer func() {
		_ = marker.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := marker.WriteString(compactionMarkerVersion); err != nil {
		return err
	}
	if err := marker.Sync(); err != nil {
		return err
	}
	if err := marker.Close(); err != nil {
		return err
	}
	if err := replaceCompactedFile(tempPath, markerPath); err != nil {
		return err
	}
	removeTemp = false
	return syncDirectory(filepath.Dir(logPath))
}
