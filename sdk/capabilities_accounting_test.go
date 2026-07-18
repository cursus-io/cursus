package sdk

import "testing"

func TestValidateNegotiatedFeaturesRequiresCompleteAccounting(t *testing.T) {
	tests := []*NegotiatedProtocol{
		{Enabled: []string{"offset_resume_v1"}},
		{Enabled: []string{"offset_resume_v1"}, Unsupported: []string{"offset_resume_v1", "structured_errors_v1"}},
		{Enabled: []string{"offset_resume_v1"}, Unsupported: []string{"unrequested_v1"}},
	}
	requested := []string{"offset_resume_v1", "structured_errors_v1"}
	for _, result := range tests {
		if err := validateNegotiatedFeatures(requested, result, false); err == nil {
			t.Fatalf("invalid feature accounting accepted: %+v", result)
		}
	}
}

func TestValidateNegotiatedFeaturesAcceptsEnabledAndUnsupportedPartition(t *testing.T) {
	result := &NegotiatedProtocol{
		Enabled:     []string{"offset_resume_v1"},
		Unsupported: []string{"future_feature_v1"},
	}
	if err := validateNegotiatedFeatures([]string{"future_feature_v1", "offset_resume_v1"}, result, false); err != nil {
		t.Fatal(err)
	}
}

func TestValidateWildcardRejectsUnsupportedResponse(t *testing.T) {
	result := &NegotiatedProtocol{Unsupported: []string{"future_feature_v1"}}
	if err := validateNegotiatedFeatures([]string{"*"}, result, false); err == nil {
		t.Fatal("wildcard response with unsupported features was accepted")
	}
}
