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
		{"cluster_metadata_unavailable", ErrorClassAvailability, true},
		{"NOT_PARTITION_LEADER", ErrorClassRouting, true},
		{"STALE_LEADER_EPOCH", ErrorClassFencing, false},
		{"partition_metadata_not_found", ErrorClassNotFound, false},
		{"invalid_commit_watermark", ErrorClassValidation, false},
		{"missing_leader_fence", ErrorClassValidation, false},
		{"leader_election_rejected", ErrorClassConflict, false},
		{"leader_election_result_unavailable", ErrorClassAvailability, true},
		{"marshal_cluster_status_failed", ErrorClassInternal, false},
		{"missing_broker", ErrorClassValidation, false},
		{"duplicate_partition", ErrorClassValidation, false},
		{"invalid_batch_commit_entry", ErrorClassValidation, false},
		{"invalid_partition", ErrorClassValidation, false},
		{"missing_generation", ErrorClassValidation, false},
	}
	for _, test := range tests {
		got := ClassifyErrorCode(test.code)
		if got.Class != test.class || got.Retryable != test.retryable {
			t.Fatalf("%s classified as %+v", test.code, got)
		}
	}
}
