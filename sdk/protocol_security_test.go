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
