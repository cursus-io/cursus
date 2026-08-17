package sdk

import (
	"strings"
	"testing"
)

func TestEncodeMessageRejectsTopicLengthOverflow(t *testing.T) {
	if encoded := EncodeMessage(strings.Repeat("t", 1<<16), "payload"); encoded != nil {
		t.Fatalf("expected nil for topic larger than uint16, got %d bytes", len(encoded))
	}
}

func TestEncodeMessageRejectsAllocationLargerThanProtocolLimit(t *testing.T) {
	if encoded := EncodeMessage("topic", strings.Repeat("p", MaxMessageSize)); encoded != nil {
		t.Fatalf("expected nil for payload larger than protocol limit, got %d bytes", len(encoded))
	}
}

func TestEncodeMessageAcceptsMaximumTopicLength(t *testing.T) {
	encoded := EncodeMessage(strings.Repeat("t", 1<<16-1), "")
	if encoded == nil {
		t.Fatal("expected maximum uint16 topic length to be accepted")
	}
}

func TestEncodeMessageAcceptsExactProtocolLimit(t *testing.T) {
	encoded := EncodeMessage("topic", strings.Repeat("p", MaxMessageSize-2-len("topic")))
	if encoded == nil || len(encoded) != MaxMessageSize {
		t.Fatalf("expected exact protocol limit to be accepted, got %d bytes", len(encoded))
	}
}
