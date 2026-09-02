package subscriber

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/consumer/config"
)

func TestEnsureConnectionNegotiatesWireV2(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	serverDone := make(chan error, 1)
	releaseServer := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()

		negotiated, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			serverDone <- err
			return
		}
		if negotiated.Compression() != wire.CompressionNone {
			serverDone <- fmt.Errorf("unexpected compression %s", negotiated.Compression())
			return
		}
		serverDone <- nil
		<-releaseServer
	}()

	cfg := &config.ConsumerConfig{
		BrokerAddrs:           []string{listener.Addr().String()},
		LeaderStaleness:       time.Second,
		MaxConnectRetries:     1,
		ConnectRetryBackoffMS: 1,
		CompressionType:       "none",
	}
	consumer, err := NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pc := &PartitionConsumer{partitionID: 0, consumer: consumer}
	t.Cleanup(func() {
		pc.close()
		close(releaseServer)
		_ = listener.Close()
	})

	if err := pc.ensureConnection(); err != nil {
		t.Fatalf("ensureConnection failed: %v", err)
	}

	pc.mu.Lock()
	conn := pc.conn
	pc.mu.Unlock()
	if _, ok := conn.(*wire.ClientConn); !ok {
		t.Fatalf("partition connection has type %T, want *wire.ClientConn", conn)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server Wire v2 handshake failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server Wire v2 handshake")
	}
}
