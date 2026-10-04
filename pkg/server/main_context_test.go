package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/topic"
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

func TestRequestDeadlineResponseReportsAcceptanceOutcome(t *testing.T) {
	timedOut, cancelTimedOut := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelTimedOut()
	clientCtx := controller.NewClientContext("", 0)
	clientCtx.SetRequestContext(timedOut)
	if got := requestDeadlineResponse("ERROR: request_cancelled", clientCtx); got != "ERROR: request_timeout outcome=not_accepted" {
		t.Fatalf("deadline response = %q, want not-accepted timeout", got)
	}
	unknownOutcome := "ERROR: request_timeout outcome=unknown"
	if got := requestDeadlineResponse(unknownOutcome, clientCtx); got != unknownOutcome {
		t.Fatalf("deadline response overwrote uncertain outcome: %q", got)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	clientCtx.SetRequestContext(canceled)
	if got := requestDeadlineResponse("ERROR: request_cancelled", clientCtx); got != "ERROR: request_cancelled" {
		t.Fatalf("cancellation response = %q, want request_cancelled", got)
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
	if err := RunServerContext(ctx, cfg, topic.NewTopicManager(cfg, nil, nil), nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRunServerContextRejectsPartialObservationCredentials(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BrokerPort = unusedTCPPort(t)
	cfg.HealthCheckPort = unusedTCPPort(t)
	cfg.ObservationGRPCPort = unusedTCPPort(t)
	cfg.ObservationGRPCPrincipal = "observer"
	cfg.LogDir = t.TempDir()
	cfg.EnableExporter = false
	cfg.EnabledDistribution = false

	err := RunServerContext(context.Background(), cfg, topic.NewTopicManager(cfg, nil, nil), nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "principal and auth token") {
		t.Fatalf("expected partial observation credential error, got %v", err)
	}
}

func TestStartObservationGRPCValidatesCredentialsAndLifecycle(t *testing.T) {
	if _, err := startObservationGRPC(context.Background(), nil); err == nil {
		t.Fatal("expected nil config to be rejected")
	}

	cfg := config.DefaultConfig()
	cfg.BrokerPort = 1
	cfg.ObservationGRPCPort = unusedTCPPort(t)
	cfg.ObservationGRPCPrincipal = "observer"
	if _, err := startObservationGRPC(context.Background(), cfg); err == nil {
		t.Fatal("expected partial observation credentials to be rejected")
	}
	occupied, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.ObservationGRPCPort))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startObservationGRPC(context.Background(), cfg); err == nil {
		_ = occupied.Close()
		t.Fatal("expected occupied observation port to be rejected")
	}
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.ObservationGRPCPrincipal = ""
	ctx, cancel := context.WithCancel(context.Background())
	shutdown, err := startObservationGRPC(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("start observation listener: %v", err)
	}

	connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.ObservationGRPCPort), time.Second)
	if err != nil {
		shutdown()
		cancel()
		t.Fatalf("dial observation listener: %v", err)
	}
	_ = connection.Close()
	cancel()
	shutdown()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		connection, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.ObservationGRPCPort), 50*time.Millisecond)
		if err != nil {
			return
		}
		_ = connection.Close()
	}
	t.Fatal("observation listener remained open after shutdown")
}

func unusedTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestInternalBrokerShutdownWaitsForWorkers(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.InternalBrokerPort = port
	cfg.InternalUseTLS = false
	cfg.MaxClientConnections = 1
	cfg.ClientIdleTimeoutMS = 25
	handler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	defer func() {
		if closeErr := handler.Close(); closeErr != nil {
			t.Errorf("close command handler: %v", closeErr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	shutdown, err := startInternalBrokerListener(ctx, cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	dialCtx, dialCancel := context.WithTimeout(context.Background(), time.Second)
	defer dialCancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		shutdown()
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	cancel()
	done := make(chan struct{})
	go func() {
		shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("internal listener shutdown did not wait for and stop workers")
	}
}
