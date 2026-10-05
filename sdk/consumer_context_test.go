package sdk

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewConsumerWithContextPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := NewConsumerWithContext(ctx, NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-consumer.mainCtx.Done():
	default:
		t.Fatal("consumer worker context was not canceled")
	}
}

func TestConsumerDoneWaitsForEachWorkerClass(t *testing.T) {
	tests := map[string]func(*Consumer) func(){
		"assignment": func(consumer *Consumer) func() {
			consumer.wg.Add(1)
			return consumer.wg.Done
		},
		"commit": func(consumer *Consumer) func() {
			consumer.commitWg.Add(1)
			return consumer.commitWg.Done
		},
		"lifecycle": func(consumer *Consumer) func() {
			consumer.lifecycleWg.Add(1)
			return consumer.lifecycleWg.Done
		},
	}

	for name, hold := range tests {
		t.Run(name, func(t *testing.T) {
			consumer, err := NewConsumer(NewDefaultConsumerConfig())
			if err != nil {
				t.Fatal(err)
			}
			release := sync.OnceFunc(hold(consumer))
			t.Cleanup(release)

			closeReturned := make(chan error, 1)
			go func() { closeReturned <- consumer.Close() }()
			deadline := time.Now().Add(time.Second)
			for consumer.State() != ConsumerStateClosing && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if consumer.State() != ConsumerStateClosing {
				t.Fatalf("Close did not enter closing state: %s", consumer.State())
			}
			select {
			case <-consumer.Done():
				t.Fatalf("Done closed while %s worker cleanup was pending", name)
			default:
			}

			release()
			select {
			case err := <-closeReturned:
				if err != nil {
					t.Fatalf("Close failed: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not finish after cleanup was released")
			}
			select {
			case <-consumer.Done():
			default:
				t.Fatal("Done remained open after cleanup")
			}
		})
	}
}

func TestNewConsumerWithContextRejectsNilContext(t *testing.T) {
	var nilContext context.Context
	if _, err := NewConsumerWithContext(nilContext, NewDefaultConsumerConfig()); err == nil {
		t.Fatal("expected nil context error")
	}
}

func TestConsumerReplacementContextKeepsRootCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := NewConsumerWithContext(ctx, NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	consumer.cancelAssignment()
	replacement := consumer.replaceAssignmentContext()
	cancel()
	select {
	case <-replacement.Done():
	default:
		t.Fatal("replacement worker context detached from root cancellation")
	}
}

func TestConsumerCloseIsIdempotentAndWaitsForShutdown(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	if !consumer.startLifecycleWorker(func() {
		<-consumer.rootCtx.Done()
		close(cleanupStarted)
		<-releaseCleanup
	}) {
		t.Fatal("lifecycle worker did not start")
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- consumer.Close() }()
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach lifecycle cleanup")
	}
	select {
	case <-consumer.Done():
		t.Fatal("Done closed before lifecycle cleanup completed")
	default:
	}
	if consumer.State() != ConsumerStateClosing {
		t.Fatalf("expected closing state during cleanup, got %s", consumer.State())
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- consumer.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("second Close returned before shutdown completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCleanup)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	select {
	case <-consumer.Done():
	default:
		t.Fatal("Done remained open after cleanup completed")
	}
	if consumer.State() != ConsumerStateClosed {
		t.Fatalf("expected closed state after Done, got %s", consumer.State())
	}
}
