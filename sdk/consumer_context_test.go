package sdk

import (
	"context"
	"testing"
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
	if _, err := NewConsumerWithContext(nil, NewDefaultConsumerConfig()); err == nil {
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
