package protocol

import "testing"

func TestSnapshotRecoveryErrorClassification(t *testing.T) {
	for _, test := range []struct {
		code      string
		class     ErrorClass
		retryable bool
	}{
		{"snapshot_generation_changed", ErrorClassAvailability, true},
		{"snapshot_lifecycle_changed", ErrorClassFencing, false},
	} {
		if !IsKnownErrorCode(test.code) {
			t.Fatalf("snapshot recovery error %s is missing from the registry", test.code)
		}
		parsed, ok := ParseErrorResponse("ERROR: " + test.code)
		if !ok || parsed.Class != test.class || parsed.Retryable != test.retryable {
			t.Fatalf("unexpected snapshot recovery classification: %+v", parsed)
		}
	}
}

func TestConsumerMetadataRecoveringClassification(t *testing.T) {
	const code = "consumer_metadata_recovering"
	if !IsKnownErrorCode(code) {
		t.Fatal("consumer recovery error is missing from the registry")
	}
	parsed, ok := ParseErrorResponse("ERROR: " + code)
	if !ok || parsed.Class != ErrorClassAvailability || !parsed.Retryable {
		t.Fatalf("unexpected consumer recovery classification: %+v", parsed)
	}
}

func TestAppendCallbacksRequireControllerFenceClassification(t *testing.T) {
	const code = "append_callbacks_require_controller_fence"
	if !IsKnownErrorCode(code) {
		t.Fatal("controller-fence contract error is missing from the registry")
	}
	parsed, ok := ParseErrorResponse("ERROR: " + code)
	if !ok || parsed.Class != ErrorClassInternal || parsed.Retryable {
		t.Fatalf("unexpected controller-fence contract classification: %+v", parsed)
	}
}

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

func TestParseErrorDerivesClassification(t *testing.T) {
	parsed, ok := ParseErrorResponse("ERROR: NOT_LEADER leader=broker-2:9000")
	if !ok || parsed.Class != ErrorClassRouting || !parsed.Retryable {
		t.Fatalf("unexpected parsed response: %+v", parsed)
	}
	if parsed.Fields["leader"] != "broker-2:9000" {
		t.Fatalf("leader field was not preserved: %+v", parsed.Fields)
	}
}

func TestParseErrorRejectsLegacyMissingColon(t *testing.T) {
	if parsed, ok := ParseErrorResponse("ERROR NOT_LEADER leader=broker-2:9000"); ok || parsed != nil {
		t.Fatalf("legacy response was accepted: %+v", parsed)
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
		{"consumer_route_fenced", ErrorClassFencing, false},
		{"transaction_manager_not_available", ErrorClassAvailability, true},
		{"replication_unavailable", ErrorClassAvailability, true},
		{"replica_offset_gap", ErrorClassAvailability, true},
		{"replica_offset_conflict", ErrorClassAvailability, true},
		{"authentication_failed", ErrorClassAuthorization, false},
		{"coordinator_not_available", ErrorClassAvailability, true},
		{"join_group_failed", ErrorClassAvailability, true},
		{"topic_materialization_pending", ErrorClassAvailability, true},
		{"cluster_metadata_unavailable", ErrorClassAvailability, true},
		{"request_cancelled", ErrorClassAvailability, true},
		{"raft_apply_topic_mismatch", ErrorClassValidation, false},
		{"NOT_PARTITION_LEADER", ErrorClassRouting, true},
		{"PARTITION_LEADER_FENCED", ErrorClassFencing, false},
		{"STALE_LEADER_EPOCH", ErrorClassFencing, false},
		{"partition_metadata_not_found", ErrorClassNotFound, false},
		{"invalid_commit_watermark", ErrorClassValidation, false},
		{"replica_index_prepare_failed", ErrorClassInternal, false},
		{"missing_leader_fence", ErrorClassValidation, false},
		{"leader_election_rejected", ErrorClassConflict, false},
		{"leader_election_result_unavailable", ErrorClassAvailability, true},
		{"marshal_cluster_status_failed", ErrorClassInternal, false},
		{"missing_broker", ErrorClassValidation, false},
		{"duplicate_partition", ErrorClassValidation, false},
		{"invalid_batch_commit_entry", ErrorClassValidation, false},
		{"invalid_partition", ErrorClassValidation, false},
		{"invalid_from_version", ErrorClassValidation, false},
		{"missing_generation", ErrorClassValidation, false},
	}
	for _, test := range tests {
		got := ClassifyErrorCode(test.code)
		if got.Class != test.class || got.Retryable != test.retryable {
			t.Fatalf("%s classified as %+v", test.code, got)
		}
	}
}
