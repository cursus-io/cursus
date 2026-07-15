package sdk

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/util"
)

func TestConsumerClientNegotiatesConfiguredProtocol(t *testing.T) {
	addr, commands, closeServer := startProtocolTestServer(t, "OK protocol_version=1 enabled=structured_errors_v1 unsupported=")
	defer closeServer()

	cfg := NewDefaultConsumerConfig()
	cfg.ProtocolVersion = 1
	cfg.ProtocolFeatures = []string{"structured_errors_v1"}
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
		want := "NEGOTIATE version=1 features=structured_errors_v1 require_features=false"
		if command != want {
			t.Fatalf("command = %q, want %q", command, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("negotiation command was not received")
	}
}

func TestProducerClientRejectsFailedRequiredNegotiation(t *testing.T) {
	addr, _, closeServer := startProtocolTestServer(t, "ERROR: UNSUPPORTED_FEATURE class=validation retryable=false features=required_v1")
	defer closeServer()

	cfg := NewDefaultPublisherConfig()
	cfg.ProtocolVersion = 1
	cfg.ProtocolFeatures = []string{"required_v1"}
	cfg.RequireProtocolFeatures = true
	client, err := NewProducerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ConnectPartition(0, addr)
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("expected BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != "UNSUPPORTED_FEATURE" || brokerErr.Retryable {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
	if client.GetConn(0) != nil {
		t.Fatal("failed negotiation stored a partition connection")
	}
}

func startProtocolTestServer(t *testing.T, response string) (string, <-chan string, func()) {
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
		defer conn.Close()
		data, err := ReadWithLength(conn)
		if err != nil {
			return
		}
		_, command, err := util.DecodeMessage(data)
		if err != nil {
			return
		}
		commands <- command
		_ = WriteWithLength(conn, []byte(response))
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
