package sdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTransactionCommandsFollowAndCacheCoordinatorRedirect(t *testing.T) {
	coordinator, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	bootstrap, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()

	_, portText, err := net.SplitHostPort(coordinator.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	serverErrors := make(chan error, 2)
	commands := make(chan string, 3)
	go serveTransactionConnections(bootstrap, 1, func(command string) string {
		commands <- "bootstrap:" + command
		return fmt.Sprintf("ERROR: NOT_COORDINATOR host=127.0.0.1 port=%d", port)
	}, serverErrors)
	go serveTransactionConnections(coordinator, 2, func(command string) string {
		commands <- "coordinator:" + command
		if strings.HasPrefix(command, "INIT_PRODUCER_ID ") {
			return "OK transactional_id=tx-redirect producerId=producer-1 epoch=3"
		}
		return "OK transactional_id=tx-redirect state=open producerId=producer-1 epoch=3"
	}, serverErrors)

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{bootstrap.Addr().String()}
	client, err := NewConsumerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	session, err := client.InitProducerID("tx-redirect")
	if err != nil {
		t.Fatalf("init producer after redirect: %v", err)
	}
	if session.ProducerID != "producer-1" || session.Epoch != 3 {
		t.Fatalf("unexpected session: %+v", session)
	}
	if err := client.BeginTransaction(session.TransactionalID, session.ProducerID, session.Epoch); err != nil {
		t.Fatalf("begin through cached coordinator: %v", err)
	}

	got := make([]string, 0, 3)
	for len(got) < 3 {
		select {
		case command := <-commands:
			got = append(got, command)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for commands: %v", got)
		}
	}
	if !strings.HasPrefix(got[0], "bootstrap:INIT_PRODUCER_ID ") {
		t.Fatalf("first command did not use bootstrap broker: %v", got)
	}
	if !strings.HasPrefix(got[1], "coordinator:INIT_PRODUCER_ID ") ||
		!strings.HasPrefix(got[2], "coordinator:BEGIN_TXN ") {
		t.Fatalf("redirect was not cached: %v", got)
	}
	for i := 0; i < 2; i++ {
		if err := <-serverErrors; err != nil {
			t.Fatal(err)
		}
	}
}

func serveTransactionConnections(listener net.Listener, count int, responder func(string) string, result chan<- error) {
	for i := 0; i < count; i++ {
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		request, err := ReadWithLength(conn)
		if err == nil {
			if len(request) < 2 {
				err = fmt.Errorf("short encoded command")
			} else {
				command := string(request[2:])
				err = WriteWithLength(conn, []byte(responder(command)))
			}
		}
		_ = conn.Close()
		if err != nil {
			result <- err
			return
		}
	}
	result <- nil
}

func TestTransactionCoordinatorRedirectRejectsInvalidAddress(t *testing.T) {
	brokerErr := &BrokerError{
		Code:   "NOT_COORDINATOR",
		Fields: map[string]string{"host": "localhost", "port": "invalid"},
	}
	client := &ConsumerClient{config: NewDefaultConsumerConfig()}
	if _, err := client.transactionCoordinatorAddress(brokerErr); err == nil {
		t.Fatal("invalid coordinator port was accepted")
	}
}

func TestTransactionalProducerReinitializesBeforeNextTransaction(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	commands := make(chan string, 5)
	serverErrors := make(chan error, 1)
	initCalls := 0
	go serveTransactionConnections(listener, 5, func(command string) string {
		commands <- command
		switch {
		case strings.HasPrefix(command, "INIT_PRODUCER_ID "):
			initCalls++
			return fmt.Sprintf("OK transactional_id=tx-sequential producerId=producer-session epoch=%d", initCalls)
		case strings.HasPrefix(command, "END_TXN "):
			return "OK transactional_id=tx-sequential state=committed messages=0 offsets=0"
		default:
			return "OK transactional_id=tx-sequential state=open"
		}
	}, serverErrors)

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	client, err := NewConsumerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := client.NewTransactionalProducer("tx-sequential")
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Begin(); err != nil {
		t.Fatal(err)
	}
	if got := producer.Session().Epoch; got != 2 {
		t.Fatalf("expected second transaction epoch 2, got %d", got)
	}

	got := make([]string, 0, 5)
	for len(got) < 5 {
		select {
		case command := <-commands:
			got = append(got, command)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for sequential commands: %v", got)
		}
	}
	if !strings.HasPrefix(got[3], "INIT_PRODUCER_ID ") || !strings.Contains(got[4], "epoch=2") {
		t.Fatalf("second transaction did not reinitialize producer: %v", got)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestTransactionalProducerPreservesSessionAcrossLifecycle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	commands := make(chan string, 7)
	serverErrors := make(chan error, 1)
	go serveTransactionConnections(listener, 7, func(command string) string {
		commands <- command
		switch {
		case strings.HasPrefix(command, "INIT_PRODUCER_ID "):
			return "OK transactional_id=tx-session producerId=producer-session epoch=4"
		case strings.HasPrefix(command, "TXN_STATUS "):
			return "OK transactional_id=tx-session state=open messages=1 offsets=2"
		case strings.HasPrefix(command, "END_TXN "):
			return "OK transactional_id=tx-session state=committed messages=1 offsets=2"
		default:
			return "OK transactional_id=tx-session state=open"
		}
	}, serverErrors)

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	client, err := NewConsumerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := client.NewTransactionalProducer("tx-session")
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish("orders", -1, Message{SeqNum: 9, Payload: "created"}); err != nil {
		t.Fatal(err)
	}
	if err := producer.SendOffsets("orders", "workers", "member-1", 2, map[int]uint64{1: 8, 0: 5}); err != nil {
		t.Fatal(err)
	}
	status, err := producer.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "open" || status.Messages != 1 || status.Offsets != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if err := producer.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Commit(); err != nil {
		t.Fatalf("commit retry failed: %v", err)
	}

	got := make([]string, 0, 7)
	for len(got) < 7 {
		select {
		case command := <-commands:
			got = append(got, command)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for lifecycle commands: %v", got)
		}
	}
	if !strings.Contains(got[2], "producerId=producer-session") ||
		!strings.Contains(got[2], "epoch=4") ||
		!strings.Contains(got[2], "partition=-1") {
		t.Fatalf("publish did not use preserved session: %s", got[2])
	}
	if !strings.HasSuffix(got[3], "P0:5,P1:8") {
		t.Fatalf("offsets were not deterministic: %s", got[3])
	}
	if got[5] != got[6] {
		t.Fatalf("commit retry changed command:\nfirst: %s\nretry: %s", got[5], got[6])
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}
