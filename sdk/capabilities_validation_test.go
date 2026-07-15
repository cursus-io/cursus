package sdk

import "testing"

func TestNormalizeFeatureNamesAllowsWildcardOnlyByItself(t *testing.T) {
	features, err := normalizeFeatureNames([]string{"*"})
	if err != nil || len(features) != 1 || features[0] != "*" {
		t.Fatalf("features=%v err=%v", features, err)
	}
	if _, err := normalizeFeatureNames([]string{"*", "structured_errors_v1"}); err == nil {
		t.Fatal("combined wildcard was accepted")
	}
}

func TestParseOKResponseRejectsDuplicateFields(t *testing.T) {
	if _, err := parseOKResponse("OK protocol_version=1 protocol_version=2"); err == nil {
		t.Fatal("duplicate field was accepted")
	}
}

func TestValidateNegotiatedFeaturesRejectsUnrequestedFeature(t *testing.T) {
	err := validateNegotiatedFeatures(
		[]string{"structured_errors_v1"},
		&NegotiatedProtocol{Enabled: []string{"offset_resume_v1"}},
		false,
	)
	if err == nil {
		t.Fatal("unrequested feature activation was accepted")
	}
}

func TestValidateNegotiatedFeaturesRequiresEveryRequestedFeature(t *testing.T) {
	err := validateNegotiatedFeatures(
		[]string{"offset_resume_v1", "structured_errors_v1"},
		&NegotiatedProtocol{Enabled: []string{"offset_resume_v1"}},
		true,
	)
	if err == nil {
		t.Fatal("missing required feature was accepted")
	}
}
