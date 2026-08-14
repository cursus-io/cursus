package disk

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestAppendMessageTimeoutDoesNotReserveOffset(t *testing.T) {
	handler := &DiskHandler{
		writeCh:      make(chan types.DiskMessage, 1),
		done:         make(chan struct{}),
		writeTimeout: time.Millisecond,
	}
	atomic.StoreUint64(&handler.AbsoluteOffset, 7)
	handler.writeCh <- types.DiskMessage{Offset: 6}

	if _, err := handler.AppendMessage("events", 0, &types.Message{Payload: "rejected"}); err == nil {
		t.Fatal("expected append timeout")
	}
	if got := handler.GetAbsoluteOffset(); got != 7 {
		t.Fatalf("failed append reserved an offset: got %d want 7", got)
	}

	<-handler.writeCh
	offset, err := handler.AppendMessage("events", 0, &types.Message{Payload: "accepted"})
	if err != nil {
		t.Fatalf("append after timeout: %v", err)
	}
	if offset != 7 {
		t.Fatalf("append after timeout got offset %d want 7", offset)
	}
	record := <-handler.writeCh
	if record.Offset != 7 {
		t.Fatalf("queued record got offset %d want 7", record.Offset)
	}
}

func TestAppendMessageRejectsOversizedSerializedRecordBeforeReservation(t *testing.T) {
	handler := &DiskHandler{
		writeCh:      make(chan types.DiskMessage, 1),
		done:         make(chan struct{}),
		writeTimeout: time.Second,
	}

	payload := strings.Repeat("x", MaxMessageSize)
	if _, err := handler.AppendMessage("events", 0, &types.Message{Payload: payload}); err == nil {
		t.Fatal("expected oversized serialized record to be rejected")
	}
	if got := handler.GetAbsoluteOffset(); got != 0 {
		t.Fatalf("oversized record reserved offset: got %d want 0", got)
	}
	if len(handler.writeCh) != 0 {
		t.Fatal("oversized record reached the write queue")
	}
}

func TestDiskAndWireMessageLimitsStayAligned(t *testing.T) {
	if MaxMessageSize <= 0 {
		t.Fatal("maximum message size must be positive")
	}
	if MaxMessageSize != 16*1024*1024 {
		t.Fatalf("unexpected durable message limit %d", MaxMessageSize)
	}
}
