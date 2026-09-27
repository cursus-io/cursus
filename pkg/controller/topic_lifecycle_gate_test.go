package controller

import (
	"context"
	"testing"
	"time"
)

func TestTopicLifecycleGateSeparatesTopicsAndCancelsWaiters(t *testing.T) {
	var gates topicLifecycleGates
	ctx := context.Background()
	releaseA, err := gates.acquire(ctx, "topic-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()

	releaseB, err := gates.acquire(ctx, "topic-b", true)
	if err != nil {
		t.Fatalf("unrelated topic gate was blocked: %v", err)
	}
	releaseB()

	writeWaitCtx, cancel := context.WithCancel(ctx)
	writeResult := make(chan error, 1)
	go func() {
		_, lockErr := gates.acquire(writeWaitCtx, "topic-a", true)
		writeResult <- lockErr
	}()
	deadline := time.After(time.Second)
	for {
		gates.mu.Lock()
		entry := gates.byName["topic-a"]
		gates.mu.Unlock()
		waiting := false
		if entry != nil {
			entry.gate.mu.Lock()
			waiting = entry.gate.waitingWriters > 0
			entry.gate.mu.Unlock()
		}
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writer did not wait on the held topic gate")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("canceled lifecycle waiter acquired the gate")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled lifecycle waiter did not return")
	}
	releaseA()

	gates.mu.Lock()
	defer gates.mu.Unlock()
	if len(gates.byName) != 0 {
		t.Fatalf("idle topic gates were retained: %d", len(gates.byName))
	}
}

func TestTopicLifecycleWriterExcludesNewReaders(t *testing.T) {
	var gates topicLifecycleGates
	readerRelease, err := gates.acquire(context.Background(), "orders", false)
	if err != nil {
		t.Fatal(err)
	}
	writerAcquired := make(chan func(), 1)
	go func() {
		release, lockErr := gates.acquire(context.Background(), "orders", true)
		if lockErr == nil {
			writerAcquired <- release
		}
	}()
	deadline := time.After(time.Second)
	for {
		gates.mu.Lock()
		entry := gates.byName["orders"]
		gates.mu.Unlock()
		waiting := false
		if entry != nil {
			entry.gate.mu.Lock()
			waiting = entry.gate.waitingWriters > 0
			entry.gate.mu.Unlock()
		}
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writer did not queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	readerAcquired := make(chan struct{}, 1)
	go func() {
		release, lockErr := gates.acquire(context.Background(), "orders", false)
		if lockErr == nil {
			release()
			readerAcquired <- struct{}{}
		}
	}()
	readerRelease()
	var releaseWriter func()
	select {
	case releaseWriter = <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire after readers drained")
	}
	select {
	case <-readerAcquired:
		t.Fatal("reader bypassed a queued lifecycle writer")
	case <-time.After(20 * time.Millisecond):
	}
	releaseWriter()
	select {
	case <-readerAcquired:
	case <-time.After(time.Second):
		t.Fatal("reader did not acquire after writer released")
	}
}
