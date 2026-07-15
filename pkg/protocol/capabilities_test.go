package protocol

import (
	"reflect"
	"testing"
)

func TestNegotiateFeatures(t *testing.T) {
	enabled, unsupported, err := NegotiateFeatures("structured_errors_v1,missing,structured_errors_v1,offset_resume_v1")
	if err != nil {
		t.Fatal(err)
	}
	wantEnabled := []Feature{FeatureOffsetResumeV1, FeatureStructuredErrorsV1}
	if !reflect.DeepEqual(enabled, wantEnabled) {
		t.Fatalf("enabled = %v, want %v", enabled, wantEnabled)
	}
	if !reflect.DeepEqual(unsupported, []string{"missing"}) {
		t.Fatalf("unsupported = %v", unsupported)
	}
}

func TestNegotiateAllFeaturesReturnsCopy(t *testing.T) {
	enabled, unsupported, err := NegotiateFeatures("*")
	if err != nil {
		t.Fatal(err)
	}
	if len(unsupported) != 0 || len(enabled) != len(SupportedFeatures()) {
		t.Fatalf("enabled=%v unsupported=%v", enabled, unsupported)
	}
	enabled[0] = "changed"
	if SupportedFeatures()[0] == "changed" {
		t.Fatal("supported feature registry escaped by reference")
	}
}
