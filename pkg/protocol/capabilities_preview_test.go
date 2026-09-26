package protocol

import "testing"

func TestPreviewReadCapabilitiesAreNegotiated(t *testing.T) {
	enabled, unsupported, err := NegotiateFeatures("browse_messages_v1,stream_history_v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(unsupported) != 0 || len(enabled) != 2 {
		t.Fatalf("enabled=%v unsupported=%v", enabled, unsupported)
	}
}
