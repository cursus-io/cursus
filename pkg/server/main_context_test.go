package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestCloseListenerOnDone(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	go closeListenerOnDone(ctx, ln)
	cancel()

	acceptDone := make(chan error, 1)
	go func() {
		_, acceptErr := ln.Accept()
		acceptDone <- acceptErr
	}()

	select {
	case acceptErr := <-acceptDone:
		if !errors.Is(acceptErr, net.ErrClosed) {
			t.Fatalf("expected closed listener error, got %v", acceptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("listener remained blocked after cancellation")
	}
}

func TestRunServerContextRejectsNilContext(t *testing.T) {
	var nilContext context.Context
	if err := RunServerContext(nilContext, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("expected nil context error")
	}
}

func TestRunServerContextReturnsCancellation(t *testing.T) {
	healthListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	healthPort := healthListener.Addr().(*net.TCPAddr).Port
	if err := healthListener.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.BrokerPort = 0
	cfg.HealthCheckPort = healthPort
	cfg.LogDir = t.TempDir()
	cfg.EnableExporter = false
	cfg.EnabledDistribution = false

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RunServerContext(ctx, cfg, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
