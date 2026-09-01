package sdk

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestConfiguredNegotiationHasBoundedTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	release := make(chan struct{})
	go func() { <-release }()

	start := time.Now()
	_, err := negotiateConfiguredProtocol(client, 2, nil, false, 25, "none")
	close(release)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("negotiation timeout was not bounded: %v", time.Since(start))
	}
}

func TestConfiguredNegotiationClearsDeadlineAfterSuccess(t *testing.T) {
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
			Payload: []byte("OK protocol=cursus min_version=2 max_version=2 default_version=2 features= error_classes=validation"),
		})
	}()

	framed, err := negotiateConfiguredProtocol(client, 2, nil, false, 25, "none")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := FetchProtocolInfo(framed); err != nil {
		t.Fatalf("cleared connection deadline was not reusable: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestFetchProtocolInfoRejectsInvalidVersionRange(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	go func() {
		_, _ = ReadWithLength(server)
		_ = WriteWithLength(server, []byte("OK protocol=cursus min_version=2 max_version=1 default_version=1 features= error_classes="))
	}()
	if _, err := FetchProtocolInfo(client); err == nil {
		t.Fatal("invalid broker protocol range was accepted")
	}
}
