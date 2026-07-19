package sdk

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"
)

func newProducerLifecycleTestHarness(t *testing.T) *Producer {
	t.Helper()
	cfg := NewDefaultPublisherConfig()
	return &Producer{
		config:    cfg,
		client:    mustNewProducerClient(cfg),
		gcTicker:  time.NewTicker(time.Hour),
		done:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}
}

func TestNewProducerWithContextRejectsNilContext(t *testing.T) {
	if _, err := NewProducerWithContext(nil, NewDefaultPublisherConfig()); err == nil {
		t.Fatal("expected nil context error")
	}
}

func TestProducerContextCancellationClosesProducer(t *testing.T) {
	producer := newProducerLifecycleTestHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	producer.closeOnContext(ctx)
	cancel()

	select {
	case <-producer.done:
	case <-time.After(time.Second):
		t.Fatal("producer remained open after context cancellation")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}
}

func TestProducerConcurrentCloseWaitsForShutdown(t *testing.T) {
	producer := newProducerLifecycleTestHarness(t)
	producer.sendersWG.Add(1)

	firstDone := make(chan error, 1)
	go func() { firstDone <- producer.Close() }()
	<-producer.done

	secondDone := make(chan error, 1)
	go func() { secondDone <- producer.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("second Close returned before shutdown completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	producer.sendersWG.Done()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func producerSenderGoroutines() int {
	stack := make([]byte, 1<<20)
	n := runtime.Stack(stack, true)
	return bytes.Count(stack[:n], []byte("sdk.(*Producer).partitionSender"))
}

func TestNewProducerCleansUpWorkersWhenAllConnectionsFail(t *testing.T) {
	before := producerSenderGoroutines()
	cfg := NewDefaultPublisherConfig()
	cfg.Topic = "producer-init-failure"
	cfg.Partitions = 2
	cfg.BrokerAddrs = []string{"127.0.0.1:0"}

	if producer, err := NewProducer(cfg); err == nil || producer != nil {
		t.Fatalf("expected connection failure, got producer=%v err=%v", producer, err)
	}

	deadline := time.Now().Add(time.Second)
	for producerSenderGoroutines() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if after := producerSenderGoroutines(); after > before {
		t.Fatalf("producer initialization leaked %d sender goroutines", after-before)
	}
}
