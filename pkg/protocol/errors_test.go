package protocol

import "testing"

func TestParseErrorResponseUsesExplicitMetadata(t *testing.T) {
	parsed, ok := ParseErrorResponse(`ERROR: broker_error class=availability retryable=true reason="temporary failure"`)
	if !ok {
		t.Fatal("response was not parsed")
	}
	if parsed.Code != "broker_error" || parsed.Class != ErrorClassAvailability || !parsed.Retryable {
		t.Fatalf("unexpected parsed response: %+v", parsed)
	}
	if parsed.Fields["reason"] != "temporary failure" || !parsed.ExplicitClassification {
		t.Fatalf("unexpected fields: %+v", parsed.Fields)
	}
}

func TestParseLegacyErrorDerivesClassification(t *testing.T) {
	parsed, ok := ParseErrorResponse("ERROR NOT_LEADER LEADER_IS broker-2:9000")
	if !ok || parsed.Class != ErrorClassRouting || !parsed.Retryable {
		t.Fatalf("unexpected parsed response: %+v", parsed)
	}
}

func TestEnrichErrorResponseIsIdempotent(t *testing.T) {
	raw := "ERROR: topic_not_found topic=orders"
	enriched := EnrichErrorResponse(raw)
	want := "ERROR: topic_not_found class=not_found retryable=false topic=orders"
	if enriched != want {
		t.Fatalf("enriched = %q, want %q", enriched, want)
	}
	if EnrichErrorResponse(enriched) != enriched {
		t.Fatal("enrichment was not idempotent")
	}
}

func TestClassifyFencingAndAvailability(t *testing.T) {
	tests := []struct {
		code      string
		class     ErrorClass
		retryable bool
	}{
		{"GEN_MISMATCH", ErrorClassFencing, false},
		{"producer_fenced", ErrorClassFencing, false},
		{"transaction_manager_not_available", ErrorClassAvailability, true},
		{"authentication_failed", ErrorClassAuthorization, false},
		{"coordinator_not_available", ErrorClassAvailability, true},
		{"invalid_partition", ErrorClassValidation, false},
	}
	for _, test := range tests {
		got := ClassifyErrorCode(test.code)
		if got.Class != test.class || got.Retryable != test.retryable {
			t.Fatalf("%s classified as %+v", test.code, got)
		}
	}
}
