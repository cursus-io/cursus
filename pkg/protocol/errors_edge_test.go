package protocol

import "testing"

func TestEnrichErrorResponseAddsOnlyMissingMetadata(t *testing.T) {
	tests := map[string]string{
		"ERROR: custom class=availability":                          "ERROR: custom retryable=false class=availability",
		"ERROR: NOT_LEADER retryable=false LEADER_IS broker-2:9000": "ERROR: NOT_LEADER class=routing retryable=false LEADER_IS broker-2:9000",
	}
	for input, want := range tests {
		if got := EnrichErrorResponse(input); got != want {
			t.Fatalf("EnrichErrorResponse(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestMalformedRetryableMetadataFallsBackToRegistry(t *testing.T) {
	parsed, ok := ParseErrorResponse("ERROR: NOT_LEADER retryable=sometimes")
	if !ok || !parsed.Retryable || parsed.RetryableExplicit {
		t.Fatalf("unexpected parsed error: %+v", parsed)
	}
}

func TestDecodeFailuresAreValidationErrors(t *testing.T) {
	classification := ClassifyErrorCode("decode_failed")
	if classification.Class != ErrorClassValidation || classification.Retryable {
		t.Fatalf("unexpected classification: %+v", classification)
	}
}
