package sdk

import (
	"context"
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
	consumer.mainCancel()
	consumer.mainCtx, consumer.mainCancel = context.WithCancel(consumer.rootCtx)
	cancel()
	select {
	case <-consumer.mainCtx.Done():
	default:
		t.Fatal("replacement worker context detached from root cancellation")
	}
}

func TestConsumerCloseIsIdempotentAndWaitsForShutdown(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	consumer.wg.Add(1)

	firstDone := make(chan error, 1)
	go func() { firstDone <- consumer.Close() }()
	<-consumer.Done()

	secondDone := make(chan error, 1)
	go func() { secondDone <- consumer.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("second Close returned before shutdown completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	consumer.wg.Done()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}
