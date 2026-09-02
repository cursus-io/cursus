package wire

import (
	"errors"
	"net"
	"testing"
)

func TestRequestFramePayloadUsesCanonicalCommandPayload(t *testing.T) {
	command, encoded, err := requestFramePayload([]byte("PUBLISH topic=orders partition=2 acks=all message=created"))
	if err != nil {
		t.Fatal(err)
	}
	if command != CommandPublish {
		t.Fatalf("command = %s, want %s", command, CommandPublish)
	}
	payload, err := DecodeCommandPayload(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Fields["topic"] != "orders" || payload.Fields["message"] != "created" {
		t.Fatalf("unexpected command payload: %+v", payload)
	}
}

func TestRequestFramePayloadRejectsLegacyTopicEnvelope(t *testing.T) {
	legacy := append([]byte{0, 0}, []byte("HELP")...)
	if _, _, err := requestFramePayload(legacy); err == nil {
		t.Fatal("legacy topic envelope was accepted")
	}
}

func TestClientReceiveReturnsStructuredBrokerError(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer func() { _ = clientNet.Close() }()
	defer func() { _ = serverNet.Close() }()

	clientCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	serverCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	client := &ClientConn{
		Conn:      clientNet,
		wire:      &Connection{conn: clientNet, codec: clientCodec},
		activeID:  7,
		activeCmd: CommandPublish,
		awaiting:  true,
	}
	client.roundTrip.Lock()
	payload, err := EncodeError(ErrorPayload{
		Code:      "NOT_LEADER",
		Class:     ErrorClassRouting,
		Retryable: true,
		Message:   "partition leader moved",
		Fields:    map[string]string{"leader": "broker-2:9092"},
	})
	if err != nil {
		t.Fatal(err)
	}
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- serverCodec.WriteFrame(serverNet, Frame{
			Kind: KindResponse, Command: CommandPublish, Status: StatusError, RequestID: 7, Payload: payload,
		})
	}()

	response, err := client.Receive()
	if response != nil {
		t.Fatalf("response = %q, want nil", response)
	}
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("error = %T %v, want *BrokerError", err, err)
	}
	if brokerErr.Code != "NOT_LEADER" || brokerErr.Class != ErrorClassRouting || !brokerErr.Retryable {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
	if brokerErr.Fields["leader"] != "broker-2:9092" {
		t.Fatalf("unexpected broker fields: %+v", brokerErr.Fields)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
}

func TestRequestFramePayloadRejectsUnsupportedAcksAlias(t *testing.T) {
	if _, _, err := requestFramePayload([]byte("PUBLISH topic=orders acks=none message=created")); err == nil {
		t.Fatal("acks=none was accepted")
	}
	if _, err := EncodeBatch(Batch{Topic: "orders", Acks: "none"}); err == nil {
		t.Fatal("batch acks=none was accepted")
	}
}

func TestClientReadStreamKeepsRoundTripUntilSecondFrame(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer func() { _ = clientNet.Close() }()
	defer func() { _ = serverNet.Close() }()
	clientCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	serverCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	client := &ClientConn{
		Conn: clientNet, wire: &Connection{conn: clientNet, codec: clientCodec},
		activeID: 9, activeCmd: CommandReadStream, awaiting: true,
	}
	client.roundTrip.Lock()
	writeErr := make(chan error, 1)
	go func() {
		for _, payload := range [][]byte{[]byte(`{"events":[]}`), []byte(`{"next_version":1}`)} {
			if err := serverCodec.WriteFrame(serverNet, Frame{
				Kind: KindResponse, Command: CommandReadStream, Status: StatusOK, RequestID: 9, Payload: payload,
			}); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	if payload, err := client.Receive(); err != nil || string(payload) != `{"events":[]}` {
		t.Fatalf("first response = %q, %v", payload, err)
	}
	if client.roundTrip.TryLock() {
		client.roundTrip.Unlock()
		t.Fatal("round trip unlocked before READ_STREAM trailer")
	}
	if payload, err := client.Receive(); err != nil || string(payload) != `{"next_version":1}` {
		t.Fatalf("second response = %q, %v", payload, err)
	}
	if !client.roundTrip.TryLock() {
		t.Fatal("round trip remained locked after READ_STREAM trailer")
	}
	client.roundTrip.Unlock()
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamKeepsRoundTripUntilStreamEnd(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer func() { _ = clientNet.Close() }()
	defer func() { _ = serverNet.Close() }()
	clientCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	serverCodec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	client := &ClientConn{
		Conn: clientNet, wire: &Connection{conn: clientNet, codec: clientCodec},
		activeID: 11, activeCmd: CommandStream, awaiting: true,
	}
	client.roundTrip.Lock()
	writeErr := make(chan error, 1)
	go func() {
		for _, frame := range []Frame{
			{Kind: KindStream, Command: CommandStream, Status: StatusOK, RequestID: 11, Payload: []byte("event")},
			{Kind: KindStream, Command: CommandStream, Status: StatusStreamEnd, RequestID: 11, Payload: []byte("STREAM_CONTROL type=CLOSE")},
		} {
			if err := serverCodec.WriteFrame(serverNet, frame); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	if payload, err := client.Receive(); err != nil || string(payload) != "event" {
		t.Fatalf("stream event = %q, %v", payload, err)
	}
	if client.roundTrip.TryLock() {
		client.roundTrip.Unlock()
		t.Fatal("round trip unlocked before stream end")
	}
	if payload, err := client.Receive(); err != nil || string(payload) != "STREAM_CONTROL type=CLOSE" {
		t.Fatalf("stream end = %q, %v", payload, err)
	}
	if !client.roundTrip.TryLock() {
		t.Fatal("round trip remained locked after stream end")
	}
	client.roundTrip.Unlock()
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
}
