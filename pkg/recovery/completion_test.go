package recovery

import (
	"context"
	"io"
	"net"
	"testing"
)

func TestResponseCompletionRetainsCanceledLeaseUntilWriterExits(t *testing.T) {
	b, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	ctx, completion := WithCompletion(ctx)
	lease := acquire(t, b, "a", 20)
	SettleAfterResponse(ctx, func() { finish(t, lease, 12) })
	cancel()
	if b.Usage().Active != 1 {
		t.Fatal("cancellation released a live response")
	}
	completion.Finish()
	completion.Finish()
	if b.Usage().Active != 0 || b.Usage().Tokens != 88 {
		t.Fatal(b.Usage())
	}
	late := acquire(t, b, "b", 0)
	SettleAfterResponse(ctx, func() { finish(t, late, 0) })
	if b.Usage().Active != 0 {
		t.Fatal("late completion leaked")
	}
}

func TestMeterCountsPartialReadsBeforeFrameFailure(t *testing.T) {
	ctx, meter := Measure(context.Background())
	reader, writer := net.Pipe()
	defer func() { _ = reader.Close() }()
	done := make(chan struct{})
	go func() { _, _ = writer.Write([]byte("partial")); _ = writer.Close(); close(done) }()
	buffer := make([]byte, 32)
	n, err := io.ReadFull(MeterConnection(ctx, reader), buffer)
	<-done
	if err != io.ErrUnexpectedEOF || n != 7 || meter.Bytes() != 7 {
		t.Fatalf("read=%d charge=%d err=%v", n, meter.Bytes(), err)
	}
}
