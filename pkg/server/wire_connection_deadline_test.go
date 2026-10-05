package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type wireTestPair struct {
	clientRaw net.Conn
	serverRaw net.Conn
	client    *wire.Connection
	server    *serverWireConn
}

func newWireTestPair(t *testing.T, writeTimeout time.Duration) wireTestPair {
	t.Helper()
	clientRaw, serverRaw := net.Pipe()
	return establishWireTestPair(t, clientRaw, serverRaw, writeTimeout)
}

func establishWireTestPair(t *testing.T, clientRaw, serverRaw net.Conn, writeTimeout time.Duration) wireTestPair {
	t.Helper()
	t.Cleanup(func() { _ = clientRaw.Close() })
	t.Cleanup(func() { _ = serverRaw.Close() })

	type serverResult struct {
		connection *wire.Connection
		err        error
	}
	result := make(chan serverResult, 1)
	go func() {
		connection, err := wire.ServerHandshake(serverRaw, []wire.Compression{wire.CompressionNone})
		result <- serverResult{connection: connection, err: err}
	}()
	client, err := wire.ClientHandshake(clientRaw, []wire.Compression{wire.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	serverHandshake := <-result
	if serverHandshake.err != nil {
		t.Fatal(serverHandshake.err)
	}
	return wireTestPair{
		clientRaw: clientRaw,
		serverRaw: serverRaw,
		client:    client,
		server:    newServerWireConn(serverRaw, serverHandshake.connection, writeTimeout),
	}
}

func TestServerWireConnTimesOutAndClosesSlowReader(t *testing.T) {
	pair := newWireTestPair(t, 40*time.Millisecond)
	requestCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	pair.server.setRequest(wire.Frame{Command: wire.CommandList, RequestID: 1}, requestCtx)

	counter := metrics.ClientResponseWriteFailures.WithLabelValues("timeout")
	before := counterValue(t, counter)
	started := time.Now()
	err := pair.server.WritePayload(bytes.Repeat([]byte("x"), 1024*1024))
	if err == nil {
		t.Fatal("slow reader response unexpectedly succeeded")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("slow reader error = %v, want timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("slow reader was held for %s", elapsed)
	}
	if got := counterValue(t, counter); got != before+1 {
		t.Fatalf("timeout metric = %v, want %v", got, before+1)
	}
	if _, err := pair.client.ReadFrame(); err == nil {
		t.Fatal("client connection remained readable after response timeout")
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func TestServerWireConnUsesRollingDeadlineForStreamFrames(t *testing.T) {
	pair := newWireTestPair(t, time.Second)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	pair.server.setRequest(wire.Frame{Command: wire.CommandStream, RequestID: 2}, requestCtx)

	frames := make(chan wire.Frame, 1)
	errs := make(chan error, 1)
	go func() {
		frame, err := pair.client.ReadFrame()
		frames <- frame
		errs <- err
	}()
	if err := pair.server.WritePayload([]byte("event")); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	frame := <-frames
	if frame.Kind != wire.KindStream || frame.RequestID != 2 || string(frame.Payload) != "event" {
		t.Fatalf("unexpected stream frame: %+v", frame)
	}
}

func TestServerWireConnWritesLargeResponseWhenClientDrains(t *testing.T) {
	pair := newWireTestPair(t, 2*time.Second)
	requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pair.server.setRequest(wire.Frame{Command: wire.CommandList, RequestID: 3}, requestCtx)
	payload := bytes.Repeat([]byte("y"), 4*1024*1024)

	frames := make(chan wire.Frame, 1)
	errs := make(chan error, 1)
	go func() {
		frame, err := pair.client.ReadFrame()
		frames <- frame
		errs <- err
	}()
	if err := pair.server.WritePayload(payload); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if frame := <-frames; frame.RequestID != 3 || !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("large response frame mismatch: request_id=%d bytes=%d", frame.RequestID, len(frame.Payload))
	}
}

func TestResponseTimeoutReleasesConnectionSlotOnce(t *testing.T) {
	limiter := newConnectionLimiter(1)
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	clientRaw, underlyingServer := net.Pipe()
	limitedServer := newLimitedConnection(underlyingServer, limiter.Release)
	pair := establishWireTestPair(t, clientRaw, limitedServer, 40*time.Millisecond)
	requestCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	pair.server.setRequest(wire.Frame{Command: wire.CommandList, RequestID: 4}, requestCtx)
	if err := pair.server.WritePayload([]byte("blocked")); err == nil {
		t.Fatal("slow reader response unexpectedly succeeded")
	}

	availableCtx, cancelAvailable := context.WithTimeout(context.Background(), time.Second)
	defer cancelAvailable()
	if err := limiter.Acquire(availableCtx); err != nil {
		t.Fatalf("connection slot was not released after write timeout: %v", err)
	}
	if err := pair.server.Close(); err != nil {
		t.Fatal(err)
	}
	doubleReleaseCtx, cancelDoubleRelease := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelDoubleRelease()
	if err := limiter.Acquire(doubleReleaseCtx); err == nil {
		t.Fatal("connection slot was released more than once")
	}
	limiter.Release()
}

func TestNegotiationWriteHonorsConnectionDeadline(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	t.Cleanup(func() { _ = clientRaw.Close() })
	t.Cleanup(func() { _ = serverRaw.Close() })
	if err := serverRaw.SetDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	codec, err := wire.NewCodec(wire.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.EncodeNegotiationRequest(wire.NegotiationRequest{
		MinimumVersion: wire.ProtocolVersion,
		MaximumVersion: wire.ProtocolVersion,
		Compressions:   []wire.Compression{wire.CompressionNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	clientWrite := make(chan error, 1)
	go func() {
		clientWrite <- codec.WriteFrame(clientRaw, wire.Frame{
			Kind: wire.KindNegotiationRequest, Command: wire.CommandNegotiate, RequestID: 5, Payload: payload,
		})
	}()
	_, _, err = negotiateServerConnection(serverRaw, 40*time.Millisecond)
	if err == nil {
		t.Fatal("negotiation response unexpectedly succeeded without a reader")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("negotiation error = %v, want timeout", err)
	}
	if err := <-clientWrite; err != nil {
		t.Fatal(err)
	}
}
