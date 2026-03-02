package disk_test

import (
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func setupDiskHandler(t *testing.T) *disk.DiskHandler {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:              tmpDir,
		DiskFlushBatchSize:  2,
		DiskFlushIntervalMS: 50,
		LingerMS:            100,
		DiskWriteTimeoutMS:  500,
		SegmentRollTimeMS:   500,
		SegmentSize:         1024,
		IndexIntervalBytes:  4096,
	}

	dh, err := disk.NewDiskHandler(cfg, "testTopic", 0)
	if err != nil {
		t.Fatalf("failed to create DiskHandler: %v", err)
	}

	return dh
}

func TestWriteDirectAndFlush(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	for i := 0; i < 3; i++ {
		msg := types.Message{
			Payload: "msg" + string(rune('A'+i)),
			SeqNum:  uint64(i),
		}
		offset, err := dh.AppendMessageSync("testTopic", 0, &msg)
		if err != nil {
			t.Fatalf("WriteDirect failed: %v", err)
		}
		if offset != uint64(i) {
			t.Errorf("expected offset %d, got %d", i, offset)
		}
	}

	dh.Flush()

	if dh.GetAbsoluteOffset() != 3 {
		t.Fatalf("expected AbsoluteOffset 3, got %d", dh.GetAbsoluteOffset())
	}

	filePath := dh.GetSegmentPath(0)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected segment file to exist at %s", filePath)
	}
}

func TestWriteBatchRotation(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	payloadSize := int(dh.SegmentSize / 2)
	payload := make([]byte, payloadSize)

	for i := range payload {
		payload[i] = 'x'
	}

	if err := dh.WriteDirect("testTopic", 0, types.Message{Offset: 0, Payload: string(payload), SeqNum: 0}); err != nil {
		t.Fatalf("WriteDirect failed: %v", err)
	}
	if err := dh.WriteDirect("testTopic", 0, types.Message{Offset: 1, Payload: string(payload), SeqNum: 1}); err != nil {
		t.Fatalf("WriteDirect failed: %v", err)
	}

	currentSeg := dh.GetCurrentSegment()
	if currentSeg != 1 {
		t.Fatalf("expected CurrentSegment 1 after rotation, got %d", currentSeg)
	}
}

func TestFlushLoopAsync(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	msgs := []struct {
		payload string
		seq     uint64
	}{
		{"async1", 1},
		{"async2", 2},
		{"async3", 3},
	}

	for _, m := range msgs {
		_, err := dh.AppendMessage("testTopic", 0, &types.Message{
			Payload: m.payload,
			SeqNum:  m.seq,
		})
		if err != nil {
			t.Fatalf("❌ failed to append message %s: %v", m.payload, err)
		}
	}

	t.Logf("waiting for flushLoop to process messages...")
	expectedOffset := uint64(len(msgs))
	success := false

	for i := 0; i < 10; i++ {
		if dh.GetAbsoluteOffset() == expectedOffset {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatalf("❌ timeout: expected AbsoluteOffset %d, got %d",
			expectedOffset, dh.GetAbsoluteOffset())
	}
}

func TestReadMessagesWithMetadata(t *testing.T) {
	dh := setupDiskHandler(t)
	defer func() { _ = dh.Close() }()

	testMessages := []struct {
		topic     string
		partition int
		offset    uint64
		payload   string
		seqNum    uint64
	}{
		{"testTopic", 0, 0, "message1", 100},
		{"testTopic", 0, 1, "message2", 101},
		{"testTopic", 0, 2, "message3", 102},
	}

	for _, tm := range testMessages {
		msg := types.Message{
			Offset:  tm.offset,
			Payload: tm.payload,
			SeqNum:  tm.seqNum,
		}
		if err := dh.WriteDirect(tm.topic, tm.partition, msg); err != nil {
			t.Fatalf("WriteDirect failed: %v", err)
		}
	}

	dh.Flush()

	messages, err := dh.ReadMessages(0, 3)
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(messages))
	}

	for i, msg := range messages {
		expected := testMessages[i]
		if msg.Offset != expected.offset {
			t.Errorf("message %d: expected offset %d, got %d", i, expected.offset, msg.Offset)
		}
		if msg.Payload != expected.payload {
			t.Errorf("message %d: expected payload %q, got %q", i, expected.payload, msg.Payload)
		}
		if msg.SeqNum != expected.seqNum {
			t.Errorf("message %d: expected seqNum %d, got %d", i, expected.seqNum, msg.SeqNum)
		}
	}
}

func TestDrainAndShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:              tmpDir,
		DiskFlushBatchSize:  2,
		DiskFlushIntervalMS: 50,
		LingerMS:            100,
		DiskWriteTimeoutMS:  500,
		SegmentRollTimeMS:   500,
		SegmentSize:         1024,
	}

	dh, err := disk.NewDiskHandler(cfg, "testTopic", 0)
	if err != nil {
		t.Fatalf("failed to create DiskHandler: %v", err)
	}

	testMsgs := []string{"shutdown_1", "shutdown_2"}
	for i, m := range testMsgs {
		offset, err := dh.AppendMessageSync("testTopic", 0, &types.Message{
			Payload: m,
			SeqNum:  uint64(i),
		})
		if err != nil {
			t.Fatalf("WriteDirect failed: %v", err)
		}
		if offset != uint64(i) {
			t.Errorf("expected offset %d, got %d", i, offset)
		}
	}

	if err := dh.Close(); err != nil {
		util.Warn("Failed to close DiskHandler: %v", err)
	}

	newDh, err := disk.NewDiskHandler(cfg, "testTopic", 0)
	if err != nil {
		t.Fatalf("failed to reopen DiskHandler: %v", err)
	}
	defer func() { _ = newDh.Close() }()

	msgs, err := newDh.ReadMessages(0, 2)
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(msgs) != len(testMsgs) {
		t.Fatalf("Data lost! expected %d, got %d", len(testMsgs), len(msgs))
	}

	for i, m := range msgs {
		if m.Payload != testMsgs[i] {
			t.Errorf("mismatch at %d: %s != %s", i, m.Payload, testMsgs[i])
		}
	}
}
