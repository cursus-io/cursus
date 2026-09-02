package sdk

import (
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestConsumerClientPerformsWireHandshake(t *testing.T) {
	addr, commands, closeServer := startWireHandshakeTestServer(t)
	defer closeServer()

	cfg := NewDefaultConsumerConfig()
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

func TestProducerClientRejectsNegativeHandshakeTimeout(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.HandshakeTimeoutMS = -1
	client, err := NewProducerClient(cfg)
	if err == nil {
		t.Fatal("negative handshake timeout was accepted")
	}
	if client != nil {
		t.Fatal("invalid Wire config created a producer client")
	}
}

func startWireHandshakeTestServer(t *testing.T) (string, <-chan string, func()) {
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
			t.Error("Wire handshake test server did not stop")
		}
	}
	return listener.Addr().String(), commands, closeServer
}
