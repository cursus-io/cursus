package sdk

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestConsumerClientNegotiatesConfiguredProtocol(t *testing.T) {
	addr, commands, closeServer := startProtocolTestServer(t)
	defer closeServer()

	cfg := NewDefaultConsumerConfig()
	cfg.ProtocolVersion = 2
	client, err := NewConsumerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	select {
	case command := <-commands:
		want := "NEGOTIATE"
		if command != want {
			t.Fatalf("command = %q, want %q", command, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("negotiation command was not received")
	}
}

func TestProducerClientRejectsLegacyProtocolFeatures(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.ProtocolVersion = 2
	cfg.ProtocolFeatures = []string{"required_v1"}
	cfg.RequireProtocolFeatures = true
	client, err := NewProducerClient(cfg)
	if err == nil || !strings.Contains(err.Error(), "legacy protocol features") {
		t.Fatalf("expected legacy feature rejection, got %v", err)
	}
	if client != nil {
		t.Fatal("invalid protocol config created a producer client")
	}
}

func startProtocolTestServer(t *testing.T) (string, <-chan string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	commands := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone}); err == nil {
			commands <- "NEGOTIATE"
		}
	}()
	closeServer := func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("protocol test server did not stop")
		}
	}
	return listener.Addr().String(), commands, closeServer
}
