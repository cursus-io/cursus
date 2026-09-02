package transport

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestRequestFramePayloadUsesCanonicalCommandPayload(t *testing.T) {
	command, encoded, err := requestFramePayload([]byte("PUBLISH topic=orders partition=2 acks=all message=created"))
	if err != nil {
		t.Fatal(err)
	}
	if command != wire.CommandPublish {
		t.Fatalf("command = %s, want %s", command, wire.CommandPublish)
	}
	payload, err := wire.DecodeCommandPayload(encoded)
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
