package disk

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func setupDiskHandlerWithIndex(t *testing.T) *DiskHandler {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:              tmpDir,
		DiskFlushBatchSize:  100,
		DiskFlushIntervalMS: 50,
		LingerMS:            100,
		DiskWriteTimeoutMS:  500,
		SegmentRollTimeMS:   500,
		SegmentSize:         1024,
		IndexIntervalBytes:  10,
		IndexSize:           1024 * 1024,
		ChannelBufferSize:   100,
	}

	dh, err := NewDiskHandler(cfg, "testTopic", 0)
	if err != nil {
		t.Fatalf("failed to create DiskHandler: %v", err)
	}

	return dh
}

func TestOpenAndCloseIndexFiles(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer func() { _ = dh.Close() }()

	indexPath := dh.GetIndexPath(0)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("index file not created at %s", indexPath)
	}

	if err := dh.CloseIndexFiles(); err != nil {
		t.Fatalf("failed to close index files: %v", err)
	}
}

func TestIndexFileErrorHandling(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer func() { _ = dh.Close() }()

	originalBaseName := dh.BaseName
	defer func() { dh.BaseName = originalBaseName }()

	dh.BaseName = "/invalid/path/that/does/not/exist"

	err := dh.OpenIndexFiles()
	if err == nil {
		t.Errorf("expected error when opening index files in invalid path")
	}
}

func TestFindOffsetPosition(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer func() { _ = dh.Close() }()

	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	offsets := make([]uint64, len(messages))

	for i, content := range messages {
		msg := &types.Message{
			Payload: content,
			SeqNum:  uint64(i),
		}
		off, err := dh.AppendMessageSync("testTopic", 0, msg)
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		offsets[i] = off
	}

	dh.Flush()

	targetOffset := offsets[2]
	pos, err := dh.findOffsetPosition(targetOffset)
	if err != nil {
		t.Fatalf("failed to find offset position: %v", err)
	}

	if pos == 0 && targetOffset > 0 {
		t.Errorf("expected non-zero position for offset %d, got 0. Check indexBytesWritten: %d", targetOffset, dh.indexBytesWritten)
	}

	messagesRead, err := dh.ReadMessages(targetOffset, 1)
	if err != nil {
		t.Fatalf("ReadMessages failed using index: %v", err)
	}

	if len(messagesRead) != 1 || messagesRead[0].Offset != targetOffset {
		t.Errorf("Index pointed to wrong position. Expected offset %d, got %v", targetOffset, messagesRead)
	}
}

func TestSeekLastValidIndexEntryScansSparseFileInBlocks(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "sparse-index-*.idx")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer func() { _ = f.Close() }()

	const fileSize = uint64(10 * 1024 * 1024)
	if err := f.Truncate(int64(fileSize)); err != nil {
		t.Fatalf("truncate index: %v", err)
	}

	entryPos := uint64(3 * types.IndexEntrySize)
	entry := make([]byte, types.IndexEntrySize)
	binary.BigEndian.PutUint64(entry[0:8], 42)
	binary.BigEndian.PutUint64(entry[8:16], 1024)
	if _, err := f.WriteAt(entry, int64(entryPos)); err != nil {
		t.Fatalf("write index entry: %v", err)
	}

	dh := &DiskHandler{}
	want := entryPos + uint64(types.IndexEntrySize)
	if got := dh.seekLastValidIndexEntry(f, fileSize); got != want {
		t.Fatalf("seekLastValidIndexEntry() = %d, want %d", got, want)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync index: %v", err)
	}

	lastOffset, count, err := getLastOffsetFromIndex(f.Name(), 0)
	if err != nil {
		t.Fatalf("getLastOffsetFromIndex() error = %v", err)
	}
	if lastOffset != 42 || count != 43 {
		t.Fatalf("getLastOffsetFromIndex() = (%d, %d), want (42, 43)", lastOffset, count)
	}
}
