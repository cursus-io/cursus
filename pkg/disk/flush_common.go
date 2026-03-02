package disk

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// flushLoop continuously processes write batches and handles segment rotation.
func (d *DiskHandler) flushLoop() {
	batch := make([]types.DiskMessage, 0, d.batchSize)
	ticker := time.NewTicker(d.linger)
	defer ticker.Stop()

	var segmentTicker *time.Ticker
	if d.segmentRollTime > 0 {
		segmentTicker = time.NewTicker(d.segmentRollTime)
		defer segmentTicker.Stop()
	}

	for {
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				d.drainAndShutdown(batch)
				return
			}

			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				util.Debug("Batch size threshold reached, flushing %d messages", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				util.Debug("Flushing %d messages on timer", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case <-d.getSegmentTickerChan(segmentTicker):
			d.mu.Lock()
			d.ioMu.Lock()
			if time.Since(d.segmentCreatedAt) >= d.segmentRollTime {
				if err := d.rotateSegment(d.AbsoluteOffset); err != nil {
					util.Error("time-based segment rotation failed: %v", err)
				}
			}
			d.ioMu.Unlock()
			d.mu.Unlock()

		case <-d.done:
			d.drainAndShutdown(batch)
			return
		}
	}
}

func (d *DiskHandler) syncLoop() {
	ticker := time.NewTicker(time.Duration(d.syncIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.ioMu.Lock()
			syncSuccess := false

			if d.file != nil {
				if err := d.file.Sync(); err == nil {
					syncSuccess = true
				} else {
					util.Error("failed to sync data file: %v", err)
					syncSuccess = false
				}
			}

			if d.indexFile != nil {
				if err := d.indexFile.Sync(); err != nil {
					util.Error("failed to sync index file: %v", err)
					syncSuccess = false
				}
			}

			currentOffset := atomic.LoadUint64(&d.AbsoluteOffset)
			d.ioMu.Unlock()

			if syncSuccess && d.OnSync != nil {
				d.OnSync(currentOffset)
			}
		case <-d.done:
			return
		}
	}
}

// WriteBatch writes a batch of messages into the current segment file.
func (d *DiskHandler) WriteBatch(batch []types.DiskMessage) error {
	if len(batch) == 0 {
		return nil
	}

	interval := d.indexInterval
	if interval == 0 {
		interval = 4096 // default interval (4KB)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return fmt.Errorf("open segment failed: %w", err)
		}
		if err := d.openIndexFiles(); err != nil {
			return err
		}
	}

	serializedMsgs := make([][]byte, 0, len(batch))
	totalSize := 0
	for i, msg := range batch {
		serialized, err := util.SerializeDiskMessage(msg)
		if err != nil {
			return fmt.Errorf("serialize failed at index %d: %w", i, err)
		}
		if len(serialized) > 0xFFFFFFFF {
			return fmt.Errorf("message too large at index %d: %d bytes", i, len(serialized))
		}
		serializedMsgs = append(serializedMsgs, serialized)
		totalSize += 4 + len(serialized)
	}

	const entrySize = uint64(types.IndexEntrySize)
	willExceedData := d.CurrentOffset+uint64(totalSize) > d.SegmentSize

	maxIndexEntries := (uint64(totalSize) / interval) + 1
	requiredIndexSpace := maxIndexEntries * entrySize
	willExceedIndex := d.indexBytesWritten+requiredIndexSpace > d.IndexSize

	if willExceedData || willExceedIndex {
		util.Debug("Rolling segment: DataExceed=%v, IndexExceed=%v, CurrentIdxBytes=%d", willExceedData, willExceedIndex, d.indexBytesWritten)
		if err := d.rotateSegment(batch[0].Offset); err != nil {
			return err
		}
	}

	accumulatedLen := uint64(0)
	lenBuf := make([]byte, 4)
	for i, serialized := range serializedMsgs {
		msgPosition := d.CurrentOffset + accumulatedLen
		msg := batch[i]

		if msgPosition-d.lastIndexPosition >= interval {
			if d.indexWriter != nil {
				entry := types.IndexEntry{
					Offset:   msg.Offset,
					Position: msgPosition,
				}

				if err := binary.Write(d.indexWriter, binary.BigEndian, entry); err != nil {
					return fmt.Errorf("failed to write index entry for offset %d: %w", msg.Offset, err)
				}

				d.indexBytesWritten += entrySize
				d.lastIndexPosition = msgPosition
			}
		}

		binary.BigEndian.PutUint32(lenBuf, uint32(len(serialized)))
		if _, err := d.writer.Write(lenBuf); err != nil {
			return fmt.Errorf("write length failed: %w", err)
		}
		if _, err := d.writer.Write(serialized); err != nil {
			return fmt.Errorf("write payload failed: %w", err)
		}
		accumulatedLen += uint64(4 + len(serialized))
	}

	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed after batch: %w", err)
	}

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			return fmt.Errorf("flush index writer failed: %w", err)
		}
	}

	d.CurrentOffset += uint64(totalSize)
	lastOffset := batch[len(batch)-1].Offset

	newAbsOffset := lastOffset + 1
	for {
		currentAbs := atomic.LoadUint64(&d.AbsoluteOffset)
		if newAbsOffset <= currentAbs {
			break
		}
		if atomic.CompareAndSwapUint64(&d.AbsoluteOffset, currentAbs, newAbsOffset) {
			break
		}
	}
	return nil
}

// WriteDirect writes a single message immediately without batching.
func (d *DiskHandler) WriteDirect(topic string, partition int, msg types.Message) error {
	interval := d.indexInterval
	if interval == 0 {
		interval = 4096 // default interval (4KB)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	diskMsg := types.DiskMessage{
		Topic:     topic,
		Partition: int32(partition),
		Offset:    msg.Offset,
		SeqNum:    msg.SeqNum,
		Epoch:     msg.Epoch,
		Payload:   msg.Payload,
	}

	serialized, err := util.SerializeDiskMessage(diskMsg)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}

	if len(serialized) > 0xFFFFFFFF {
		return fmt.Errorf("message too large: %d bytes", len(serialized))
	}

	totalLen := uint64(4 + len(serialized))
	const entrySize = uint64(types.IndexEntrySize)
	msgPosition := d.CurrentOffset

	willWriteIndex := msgPosition-d.lastIndexPosition >= interval
	willExceedData := d.CurrentOffset+totalLen > d.SegmentSize
	willExceedIndex := willWriteIndex && (d.indexBytesWritten+entrySize > d.IndexSize)

	if willExceedData || willExceedIndex {
		util.Debug("rolling segment: data exceed=%v, index exceed=%v", willExceedData, willExceedIndex)
		if err := d.rotateSegment(msg.Offset); err != nil {
			return fmt.Errorf("rotateSegment failed: %w", err)
		}
		msgPosition = d.CurrentOffset
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(serialized)))

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := d.writer.Write(serialized); err != nil {
		return err
	}
	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	d.CurrentOffset += totalLen
	if msg.Offset >= atomic.LoadUint64(&d.AbsoluteOffset) {
		atomic.StoreUint64(&d.AbsoluteOffset, msg.Offset+1)
	}

	if msgPosition-d.lastIndexPosition >= interval {
		indexEntry := types.IndexEntry{
			Offset:   msg.Offset,
			Position: msgPosition,
		}
		if d.indexWriter != nil {
			if err := binary.Write(d.indexWriter, binary.BigEndian, indexEntry); err != nil {
				return fmt.Errorf("failed to write index entry for offset %d: %w", msg.Offset, err)
			}
			if err := d.indexWriter.Flush(); err != nil {
				return fmt.Errorf("failed to flush index writer: %w", err)
			}
			d.lastIndexPosition = msgPosition
			d.indexBytesWritten += uint64(types.IndexEntrySize)
		}
	}
	return nil
}

// rotateSegment closes the current segment and opens a new one.
func (d *DiskHandler) rotateSegment(nextBaseOffset uint64) error {
	if nextBaseOffset <= d.CurrentSegment && d.CurrentOffset == 0 {
		return nil
	}

	var errs []error
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("failed to sync disk file: %v", err)
		}
		if err := d.file.Close(); err != nil {
			util.Error("close failed during rotation: %v", err)
			errs = append(errs, err)
		}
		d.file = nil
	}

	d.indexMu.Lock()
	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			util.Error("flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}
	if err := d.closeIndexFiles(); err != nil {
		util.Error("close index files failed during rotation: %v", err)
		errs = append(errs, err)
	}
	d.indexMu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("rotation completed with errors: %v", errs)
	}

	d.CurrentSegment = nextBaseOffset
	d.CurrentOffset = 0
	d.lastIndexPosition = 0
	d.indexBytesWritten = 0
	d.segmentCreatedAt = time.Now()

	d.segments = append(d.segments, nextBaseOffset)
	sort.Slice(d.segments, func(i, j int) bool {
		return d.segments[i] < d.segments[j]
	})

	if err := d.openSegment(); err != nil {
		util.Error("Failed to open new segment: %v", err)
		return err
	}
	return d.openIndexFiles()
}

// Flush forces all pending data to be written and synced to disk.
func (d *DiskHandler) Flush() {
	pendingLen := len(d.writeCh)
	batch := make([]types.DiskMessage, 0, pendingLen)

	for range len(d.writeCh) {
		select {
		case msg := <-d.writeCh:
			batch = append(batch, msg)
		default:
			goto perform_write
		}
	}

perform_write:
	if len(batch) > 0 {
		if err := d.WriteBatch(batch); err != nil {
			util.Error("Flush write failed: %v", err)
			return
		}
		return
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("flush failed in Flush: %v", err)
		}
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("failed to sync disk file: %v", err)
		}
	}
}

// GetAbsoluteOffset returns the current absolute offset in a thread-safe manner
func (d *DiskHandler) GetAbsoluteOffset() uint64 {
	return atomic.LoadUint64(&d.AbsoluteOffset)
}

// GetCurrentSegment returns the current segment number in a thread-safe manner
func (d *DiskHandler) GetCurrentSegment() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CurrentSegment
}

func (d *DiskHandler) drainAndShutdown(batch []types.DiskMessage) {
	for {
		stop := false
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				stop = true
			} else {
				batch = append(batch, msg)
			}
		default:
			stop = true
		}

		if len(batch) >= d.batchSize {
			if err := d.WriteBatch(batch); err != nil {
				util.Error("WriteBatch failed: %v", err)
			}
			batch = batch[:0]
		}

		if stop {
			break
		}
	}

	if len(batch) > 0 {
		if err := d.WriteBatch(batch); err != nil {
			util.Error("finalize WriteBatch failed: %v", err)
		}
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("writer flush failed: %v", err)
		}
		d.writer = nil
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("file sync failed: %v", err)
		}
		if err := d.file.Close(); err != nil {
			util.Error("file close failed: %v", err)
		}
		d.file = nil
	}

	if err := d.closeIndexFiles(); err != nil {
		util.Error("close index files failed during shutdown: %v", err)
	}
}

func (d *DiskHandler) getSegmentTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}
