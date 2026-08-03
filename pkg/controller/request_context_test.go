package controller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClientContextRequestContext(t *testing.T) {
	clientCtx := NewClientContext("group", 0)
	if clientCtx.RequestContext() == nil {
		t.Fatal("default request context is nil")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	clientCtx.SetRequestContext(requestCtx)
	cancel()
	if !errors.Is(clientCtx.RequestContext().Err(), context.Canceled) {
		t.Fatalf("request context error = %v, want context.Canceled", clientCtx.RequestContext().Err())
	}

	clientCtx.SetRequestContext(nil)
	if err := clientCtx.RequestContext().Err(); err != nil {
		t.Fatalf("nil context should restore background context, got %v", err)
	}
}

func TestWaitForContextStopsOnCancellation(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()

	err := waitForContext(requestCtx, time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForContext error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled wait took %v", elapsed)
	}
}

func TestApplyAndWaitContextRejectsCancelledRequestFirst(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	handler := &CommandHandler{}
	_, err := handler.applyAndWaitContext(requestCtx, "TEST", map[string]interface{}{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("applyAndWaitContext error = %v, want context.Canceled", err)
	}
}

func TestPublishTopicWaitStopsOnRequestCancellation(t *testing.T) {
	handler, _ := newTestHandler(t)
	clientCtx := NewClientContext("group", 0)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()

	response := handler.HandleCommandContext(requestCtx, "PUBLISH topic=missing producerId=p1 message=value", clientCtx)
	if response != "ERROR: request_cancelled" {
		t.Fatalf("response = %q, want request cancellation", response)
	}
	if err := clientCtx.RequestContext().Err(); err != nil {
		t.Fatalf("connection context was not restored: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancelled publish took %v", elapsed)
	}
}
