package sdk

import (
	"errors"
	"net"
	"strings"
	"testing"
)

func TestExecuteTransactionCommandPreservesStructuredBrokerError(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		request, err := ReadWithLength(server)
		if err != nil {
			serverDone <- err
			return
		}
		if got := string(request[2:]); got != "END_TXN transactional_id=tx-1 producerId=p-1 epoch=2 result=commit" {
			serverDone <- errors.New("unexpected transaction command: " + got)
			return
		}
		serverDone <- WriteWithLength(server, []byte("ERROR: producer_fenced class=fencing retryable=false current_epoch=3 requested_epoch=2"))
	}()

	_, err := executeTransactionCommand(client, "END_TXN transactional_id=tx-1 producerId=p-1 epoch=2 result=commit")
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("expected typed BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != "producer_fenced" || brokerErr.Class != ErrorClassFencing || brokerErr.Retryable {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestExecuteTransactionCommandRequiresExactOKToken(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = ReadWithLength(server)
		_ = WriteWithLength(server, []byte("OKAY state=committed"))
	}()

	if _, err := executeTransactionCommand(client, "TXN_STATUS transactional_id=tx-1"); err == nil {
		t.Fatal("broad OK prefix was accepted")
	}
}

func TestBuildSendOffsetsToTransactionCommandSortsPartitions(t *testing.T) {
	cmd, err := buildSendOffsetsToTransactionCommand(
		"tx-1", "producer-1", "orders", "workers", "member-1", 4, 2,
		map[int]uint64{10: 90, 2: 30, 0: 11},
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := "SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=producer-1 epoch=2 topic=orders group=workers member=member-1 generation=4 P0:11,P2:30,P10:90"
	if cmd != expected {
		t.Fatalf("command mismatch\nwant: %s\n got: %s", expected, cmd)
	}
}

func TestBuildSendOffsetsToTransactionCommandRejectsUnsafeInputs(t *testing.T) {
	tests := []struct {
		name    string
		txnID   string
		group   string
		offsets map[int]uint64
	}{
		{name: "empty offsets", txnID: "tx-1", group: "workers", offsets: map[int]uint64{}},
		{name: "negative partition", txnID: "tx-1", group: "workers", offsets: map[int]uint64{-1: 1}},
		{name: "whitespace identifier", txnID: "tx 1", group: "workers", offsets: map[int]uint64{0: 1}},
		{name: "whitespace group", txnID: "tx-1", group: "worker pool", offsets: map[int]uint64{0: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildSendOffsetsToTransactionCommand(test.txnID, "producer-1", "orders", test.group, "member-1", 1, 0, test.offsets)
			if err == nil {
				t.Fatal("unsafe input was accepted")
			}
		})
	}
}

func TestParseTransactionStatus(t *testing.T) {
	status, err := parseTransactionStatus("OK transactional_id=tx-1 state=committed messages=2 offsets=3")
	if err != nil {
		t.Fatal(err)
	}
	if status.TransactionalID != "tx-1" || status.State != "committed" || status.Messages != 2 || status.Offsets != 3 {
		t.Fatalf("unexpected status: %+v", status)
	}

	for _, response := range []string{
		"OK transactional_id=tx-1 messages=2 offsets=3",
		"OK transactional_id=tx-1 state=committed messages=-1 offsets=3",
		"OK transactional_id=tx-1 state=committed messages=two offsets=3",
		"OKAY transactional_id=tx-1 state=committed messages=2 offsets=3",
	} {
		if _, err := parseTransactionStatus(response); err == nil {
			t.Fatalf("invalid status accepted: %s", response)
		}
	}
}

func TestTransactionValidationStopsBeforeDial(t *testing.T) {
	client := &ConsumerClient{config: &ConsumerConfig{}}
	if _, err := client.InitProducerID("bad id"); err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("unexpected transactional ID validation: %v", err)
	}
	if err := client.TransactionalPublish("tx-1", "orders", -2, Message{ProducerID: "p-1", SeqNum: 1, Payload: "payload"}); err == nil {
		t.Fatal("partition below the auto-assignment sentinel was accepted")
	}
	if err := client.TransactionalPublish("tx-1", "orders", -1, Message{ProducerID: "p-1", SeqNum: 1, Payload: "payload"}); err == nil || strings.Contains(err.Error(), "partition") {
		t.Fatalf("auto-assignment partition sentinel was rejected before dial: %v", err)
	}
	if err := client.TransactionalPublish("tx-1", "orders", 0, Message{ProducerID: "p-1", SeqNum: 0, Payload: "payload"}); err == nil {
		t.Fatal("zero sequence was accepted")
	}
}
