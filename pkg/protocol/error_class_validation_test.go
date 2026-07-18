package protocol

import "testing"

func TestUnknownExplicitErrorClassFailsClosed(t *testing.T) {
	parsed, ok := ParseErrorResponse("ERROR: custom class=surprise retryable=true")
	if !ok {
		t.Fatal("error response was not parsed")
	}
	if parsed.Class != ErrorClassInternal || parsed.Retryable || parsed.ExplicitClassification {
		t.Fatalf("unknown class did not fail closed: %+v", parsed)
	}
}

func TestKnownExplicitErrorClassCanOverrideDefault(t *testing.T) {
	parsed, ok := ParseErrorResponse("ERROR: custom class=availability retryable=true")
	if !ok || parsed.Class != ErrorClassAvailability || !parsed.Retryable || !parsed.ExplicitClassification {
		t.Fatalf("known explicit classification was not preserved: %+v", parsed)
	}
}
