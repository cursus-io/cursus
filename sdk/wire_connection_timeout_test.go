package sdk

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestWireHandshakeHasBoundedTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	release := make(chan struct{})
	go func() { <-release }()

	start := time.Now()
	_, err := openWireConnection(client, 25, "none")
	close(release)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("handshake timeout was not bounded: %v", time.Since(start))
	}
}

func TestWireHandshakeClearsDeadlineAfterSuccess(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	serverDone := make(chan error, 1)
	go func() {
		connection, err := wire.ServerHandshake(server, []wire.Compression{wire.CompressionNone})
		if err != nil {
			serverDone <- err
			return
		}
		request, err := connection.ReadFrame()
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- connection.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID,
			Payload: []byte("OK commands=HELP"),
		})
	}()

	framed, err := openWireConnection(client, 25, "none")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := WriteWithLength(framed, []byte("HELP")); err != nil {
		t.Fatalf("cleared connection deadline was not reusable: %v", err)
	}
	if _, err := ReadWithLength(framed); err != nil {
		t.Fatalf("cleared connection deadline was not reusable: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
