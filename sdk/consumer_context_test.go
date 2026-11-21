package sdk

import (
	"context"
	"errors"
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

func TestConsumerCloseContextReturnsAtDeadline(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	consumer.lifecycleWg.Add(1)
	go func() {
		defer consumer.lifecycleWg.Done()
		<-release
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = consumer.CloseContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected close deadline, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("CloseContext exceeded bounded shutdown: %v", elapsed)
	}
	if consumer.State() != ConsumerStateClosed {
		t.Fatalf("expected closed state, got %s", consumer.State())
	}
	select {
	case <-consumer.Done():
	default:
		t.Fatal("Done remained open after bounded shutdown")
	}
	close(release)
}

func TestConsumerCloseDeadlineDoesNotCloseWorkerOwnedChannels(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	consumer.state.Store(uint32(ConsumerStateRunning))
	consumer.assignmentGeneration.Store(1)
	pc := &PartitionConsumer{
		partitionID:          0,
		consumer:             consumer,
		assignmentGeneration: 1,
		ctx:                  consumer.assignmentContext(),
		dataCh:               make(chan *messageBatch, 1),
	}
	consumer.partitionConsumers[0] = pc

	release := make(chan struct{})
	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		<-release
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = consumer.CloseContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected close deadline, got %v", err)
	}

	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 1}}}
	consumer.commitCh <- commitEntry{partition: 0, offset: 2, assignmentGeneration: 1}
	close(release)
	if err := waitGroupContext(context.Background(), &consumer.wg); err != nil {
		t.Fatal(err)
	}
	pc.closeDataCh()
}

func TestConsumerCloseContextStopsCooperativeWorkers(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		<-consumer.rootCtx.Done()
	}()
	consumer.lifecycleWg.Add(1)
	go func() {
		defer consumer.lifecycleWg.Done()
		<-consumer.rootCtx.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := consumer.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitGroupContext(context.Background(), &consumer.wg); err != nil {
		t.Fatal(err)
	}
	if err := waitGroupContext(context.Background(), &consumer.lifecycleWg); err != nil {
		t.Fatal(err)
	}
}
