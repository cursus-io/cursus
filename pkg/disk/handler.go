package disk

import (
	"bufio"
	"encoding/binary"
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
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"golang.org/x/exp/mmap"
)

const MaxMessageSize = 16 * 1024 * 1024 // 16MB

type DiskHandler struct {
	BaseName       string
	SegmentSize    uint64
	IndexSize      uint64
	CurrentOffset  uint64
	AbsoluteOffset uint64
	CurrentSegment uint64
	segments       []uint64

	indexFile         *os.File
	indexWriter       *bufio.Writer
	indexMapper       *mmap.ReaderAt
	indexInterval     uint64
	indexBytesWritten uint64
	indexMu           sync.RWMutex
	activeReaders     int32
	lastIndexPosition uint64

	writeCh chan types.DiskMessage
	done    chan struct{}
	OnSync  func(uint64)

	batchSize      int
	linger         time.Duration
	writeTimeout   time.Duration
	syncIntervalMS int

	segmentRollTime  time.Duration
	segmentCreatedAt time.Time

	mu   sync.Mutex // metadata(offset, segment), file handler(d.file)
	ioMu sync.Mutex // bufio.Writer, flush

	file   *os.File
	writer *bufio.Writer

	closeOnce sync.Once
	shutdown  sync.WaitGroup
}

func (d *DiskHandler) GetActiveReaders() int32 {
	return atomic.LoadInt32(&d.activeReaders)
}

func NewDiskHandler(cfg *config.Config, topicName string, partitionID int) (*DiskHandler, error) {
	base := fmt.Sprintf("%s%c%s%cpartition_%d", cfg.LogDir, os.PathSeparator, topicName, os.PathSeparator, partitionID)
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		return nil, err
	}

	tempDh := &DiskHandler{BaseName: base}
	pattern := base + "_segment_*.log"
	files, _ := filepath.Glob(pattern)
	sort.Strings(files)

	var lastAbsoluteOffset uint64
	var currentSegmentBase uint64
	prefix := fmt.Sprintf("partition_%d_segment_", partitionID)

	if len(files) > 0 {
		lastFile := files[len(files)-1]
		fileName := filepath.Base(lastFile)

		if strings.HasPrefix(fileName, prefix) {
			numStr := fileName[len(prefix) : len(fileName)-4]

			var err error
			currentSegmentBase, err = strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("critical: failed to parse last segment filename %s: %w", fileName, err)
			}
		}

		indexPath := tempDh.GetIndexPath(currentSegmentBase)
		lastOffset, _, err := getLastOffsetFromIndex(indexPath, currentSegmentBase)
		if err == nil {
			lastAbsoluteOffset = lastOffset + 1
		} else {
			c, countErr := countMessagesInFile(lastFile) // fallback
			if countErr != nil {
				util.Error("failed to count messages in last segment %s: %v", lastFile, countErr)
			}
			lastAbsoluteOffset = currentSegmentBase + uint64(c)
		}
	}

	filePath := tempDh.GetSegmentPath(currentSegmentBase)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	currentOffset := uint64(fileInfo.Size())

	dh := &DiskHandler{
		BaseName:       base,
		SegmentSize:    uint64(cfg.SegmentSize),
		IndexSize:      uint64(cfg.IndexSize),
		CurrentSegment: currentSegmentBase,
		segments:       make([]uint64, 0),
		CurrentOffset:  currentOffset,
		AbsoluteOffset: lastAbsoluteOffset,

		indexInterval: uint64(cfg.IndexIntervalBytes),

		writeCh:        make(chan types.DiskMessage, cfg.ChannelBufferSize),
		done:           make(chan struct{}),
		batchSize:      cfg.DiskFlushBatchSize,
		linger:         time.Duration(cfg.LingerMS) * time.Millisecond,
		writeTimeout:   time.Duration(cfg.DiskWriteTimeoutMS) * time.Millisecond,
		syncIntervalMS: cfg.DiskFlushIntervalMS,

		segmentRollTime:  time.Duration(cfg.SegmentRollTimeMS) * time.Millisecond,
		segmentCreatedAt: time.Now(),
		file:             file,
		writer:           bufio.NewWriter(file),
	}

	if err := dh.openIndexFiles(); err != nil {
		return nil, err
	}

	for _, f := range files {
		fileName := filepath.Base(f)
		if strings.HasPrefix(fileName, prefix) {
			numStr := fileName[len(prefix) : len(fileName)-4]
			segBase, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				util.Error("skipping invalid segment file %s: %v", fileName, err)
				continue
			}
			dh.segments = append(dh.segments, segBase)
		}
	}

	if len(dh.segments) == 0 {
		dh.segments = append(dh.segments, currentSegmentBase)
	}
	sort.Slice(dh.segments, func(i, j int) bool { return dh.segments[i] < dh.segments[j] })

	dh.shutdown.Add(2)
	go func() {
		defer dh.shutdown.Done()
		dh.flushLoop()
	}()

	go func() {
		defer dh.shutdown.Done()
		dh.syncLoop()
	}()

	dh.shutdown.Add(1)
	go func() {
		defer dh.shutdown.Done()
		dh.retentionLoop(cfg)
	}()

	return dh, nil
}

func countMessagesInFile(filePath string) (int, error) {
	reader, err := mmap.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			util.Error("failed to close: %v", err)
		}
	}()

	count := 0
	pos := 0
	for pos+4 <= reader.Len() {
		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			return count, fmt.Errorf("failed to read length at pos %d: %w", pos, err)
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)
		if msgLen == 0 || msgLen > MaxMessageSize {
			util.Error("Corrupted data: invalid msgLen %d at pos %d", msgLen, pos)
			break
		}

		if pos+4+int(msgLen) > reader.Len() {
			util.Error("⚠️ Incomplete message at pos %d in %s (expected %d bytes, file ends at %d)",
				pos, filePath, msgLen, reader.Len())
			break
		}

		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}

func (d *DiskHandler) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	d.mu.Lock()
	offset := d.AbsoluteOffset
	d.AbsoluteOffset++
	d.mu.Unlock()

	msg.Offset = offset
	if err := d.WriteDirect(topic, partition, *msg); err != nil {
		return 0, fmt.Errorf("WriteDirect failed: %w", err)
	}
	return offset, nil
}

// AppendMessage sends a message to the internal write channel for asynchronous disk persistence.
func (d *DiskHandler) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	d.mu.Lock()
	offset := d.AbsoluteOffset
	d.AbsoluteOffset++
	d.mu.Unlock()

	msg.Offset = offset
	diskMsg := types.DiskMessage{
		Topic:      topic,
		Partition:  int32(partition),
		Offset:     offset,
		ProducerID: msg.ProducerID,
		SeqNum:     msg.SeqNum,
		Epoch:      msg.Epoch,
		Payload:    msg.Payload,
	}

	if d.writeTimeout > 0 {
		timer := time.NewTimer(d.writeTimeout)
		defer timer.Stop()

		select {
		case <-d.done:
			util.Debug("done channel closed for %s", d.BaseName)
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			return diskMsg.Offset, nil
		case <-timer.C:
			util.Error("enqueue timed out after %s for topic %s", d.writeTimeout, topic)
			return 0, fmt.Errorf("enqueue timeout after %s", d.writeTimeout)
		}
	} else {
		select {
		case <-d.done:
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			return diskMsg.Offset, nil
		}
	}
}

// ReadMessages reads a batch of messages from the disk log, starting from the given offset.
func (dh *DiskHandler) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	atomic.AddInt32(&dh.activeReaders, 1)
	defer atomic.AddInt32(&dh.activeReaders, -1)

	_, targetSeg, err := dh.findSegmentForOffset(offset)
	if err != nil {
		util.Error("Segment not found for offset %d", offset)
		return nil, err
	}

	position, err := dh.findOffsetPosition(offset)
	if err != nil {
		util.Error("Position not found for offset %d", offset)
		return nil, err
	}

	dh.mu.Lock()
	readableSegments := make([]uint64, len(dh.segments))
	copy(readableSegments, dh.segments)
	if len(readableSegments) == 0 || readableSegments[len(readableSegments)-1] != dh.CurrentSegment {
		readableSegments = append(readableSegments, dh.CurrentSegment)
	}
	dh.mu.Unlock()

	var messages []types.Message
	startReading := false

	for _, segBase := range readableSegments {
		if !startReading {
			if segBase == targetSeg {
				startReading = true
			} else {
				continue
			}
		}

		currentFile := dh.GetSegmentPath(segBase)
		fi, err := os.Stat(currentFile)
		if err != nil {
			util.Debug("stat failed for %s: %v", currentFile, err)
			continue
		}

		actualSize := fi.Size()
		if actualSize == 0 {
			util.Debug("Segment %d is currently empty, skipping", segBase)
			continue
		}

		reader, err := mmap.Open(currentFile)
		if err != nil {
			util.Debug("mmap open failed: %v", err)
			continue
		}

		remaining := max - len(messages)
		readPos := uint64(0)
		if segBase == targetSeg {
			readPos = position
		}

		batch := dh.readMessagesFromPosition(reader, readPos, remaining, offset)
		if len(batch) == 0 && uint64(actualSize) > readPos+4 {
			if err := reader.Close(); err != nil {
				util.Debug("error closing reader: %v", err)
			}

			var reErr error
			reader, reErr = mmap.Open(currentFile)
			if reErr != nil {
				util.Error("mmap re-open failed for %s: %v", currentFile, reErr)
				continue
			}

			batch = dh.readMessagesFromPosition(reader, readPos, remaining, offset)
		}

		messages = append(messages, batch...)
		if err := reader.Close(); err != nil {
			util.Debug("error closing reader: %v", err)
		}

		if len(messages) >= max {
			break
		}
		if len(messages) > 0 {
			offset = messages[len(messages)-1].Offset + 1
		}
	}

	if len(messages) > 0 {
		first := messages[0].Offset
		last := messages[len(messages)-1].Offset
		util.Debug("Success: From=%d To=%d (Count=%d, Range: [%d-%d])", first, last, len(messages), messages[0].Offset, messages[len(messages)-1].Offset)
	}
	return messages, nil
}

// readMessagesFromPosition reads messages starting from a specific byte position
func (dh *DiskHandler) readMessagesFromPosition(reader *mmap.ReaderAt, position uint64, max int, targetOffset uint64) []types.Message {
	messages := []types.Message{}
	pos := int(position)

	for len(messages) < max && pos+4 <= reader.Len() {
		lenBytes := make([]byte, 4)
		if _, err := reader.ReadAt(lenBytes, int64(pos)); err != nil {
			break
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)
		if msgLen == 0 || msgLen > MaxMessageSize {
			util.Error("Corrupted message length detected at pos %d: %d bytes (limit: %d)", pos, msgLen, MaxMessageSize)
			break
		}

		if pos+4+int(msgLen) > reader.Len() {
			util.Error("Incomplete message at pos %d: expected %d bytes but reached EOF", pos, msgLen)
			break
		}
		data := make([]byte, msgLen)
		if _, err := reader.ReadAt(data, int64(pos+4)); err != nil {
			break
		}

		diskMsg, err := util.DeserializeDiskMessage(data)
		if err != nil {
			util.Error("failed to deserialize disk message at pos %d: %v", pos, err)
			pos += 4 + int(msgLen)
			continue
		}

		if diskMsg.Offset < targetOffset {
			pos += 4 + int(msgLen)
			continue
		}

		messages = append(messages, types.Message{
			Offset:     diskMsg.Offset,
			Payload:    diskMsg.Payload,
			ProducerID: diskMsg.ProducerID,
			SeqNum:     diskMsg.SeqNum,
			Epoch:      diskMsg.Epoch,
		})
		pos += 4 + int(msgLen)
	}
	return messages
}

func (d *DiskHandler) countMessagesInSegment() (int, error) {
	return d.countMessagesInSegmentID(d.CurrentSegment)
}

func (d *DiskHandler) countMessagesInSegmentID(segmentID uint64) (int, error) {
	filePath := d.GetSegmentPath(segmentID)
	reader, err := mmap.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("mmap open failed: %w", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			util.Error("failed to close: %v", err)
		}
	}()

	count := 0
	pos := 0
	for pos < reader.Len() {
		if pos+4 > reader.Len() {
			break
		}

		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			break
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)
		if msgLen == 0 || msgLen > MaxMessageSize {
			util.Error("Corrupted data: invalid msgLen %d at pos %d", msgLen, pos)
			break
		}

		if pos+4+int(msgLen) > reader.Len() {
			break
		}
		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}
