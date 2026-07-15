package protocol

import (
	"strings"
	"testing"
)

func TestParseFeatureRequestRejectsUnsafeOrOversizedValues(t *testing.T) {
	tests := []string{
		"structured-errors-v1",
		"structured_errors_v1,*",
		"structured_errors_v1 require_features=false",
		strings.Repeat("a", MaxFeatureNameLength+1),
	}
	for _, value := range tests {
		if _, _, err := ParseFeatureRequest(value); err == nil {
			t.Fatalf("unsafe feature request accepted: %q", value)
		}
	}
}

func TestParseFeatureRequestBoundsEntryCount(t *testing.T) {
	values := make([]string, MaxRequestedFeatures+1)
	for i := range values {
		values[i] = "feature_" + strings.Repeat("x", i%4)
	}
	if _, _, err := ParseFeatureRequest(strings.Join(values, ",")); err == nil {
		t.Fatal("oversized feature list was accepted")
	}
}

func TestNegotiateFeaturesRejectsInvalidName(t *testing.T) {
	if _, _, err := NegotiateFeatures("invalid-feature"); err == nil {
		t.Fatal("invalid feature name was accepted")
	}
}
