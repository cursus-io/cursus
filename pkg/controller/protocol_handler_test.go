package controller

import (
	"strings"
	"testing"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

func TestProtocolInfoAdvertisesStableCapabilities(t *testing.T) {
	ch, _ := newTestHandler(t)
	resp := ch.HandleCommand("PROTOCOL_INFO", NewClientContext("", 0))
	for _, expected := range []string{
		"OK protocol=cursus",
		"min_version=1",
		"max_version=1",
		"structured_errors_v1",
		"error_classes=authorization,availability,conflict,fencing,internal,not_found,routing,validation",
	} {
		if !strings.Contains(resp, expected) {
			t.Fatalf("PROTOCOL_INFO response %q missing %q", resp, expected)
		}
	}
}

func TestNegotiateEnablesStructuredErrorsPerConnection(t *testing.T) {
	ch, _ := newTestHandler(t)
	legacy := NewClientContext("", 0)
	negotiated := NewClientContext("", 0)

	resp := ch.HandleCommand("NEGOTIATE version=1 features=structured_errors_v1,missing", negotiated)
	if resp != "OK protocol_version=1 enabled=structured_errors_v1 unsupported=missing" {
		t.Fatalf("unexpected negotiation response: %s", resp)
	}
	if !negotiated.HasFeature(wireprotocol.FeatureStructuredErrorsV1) {
		t.Fatal("structured error feature was not enabled")
	}

	legacyError := ch.HandleCommand("DELETE topic=missing", legacy)
	if strings.Contains(legacyError, "class=") {
		t.Fatalf("legacy response changed: %s", legacyError)
	}
	structuredError := ch.HandleCommand("DELETE topic=missing", negotiated)
	want := "ERROR: topic_not_found class=not_found retryable=false topic=missing"
	if structuredError != want {
		t.Fatalf("structured error = %q, want %q", structuredError, want)
	}
}

func TestNegotiateRejectsUnsupportedRequiredFeaturesWithoutChangingContext(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	resp := ch.HandleCommand("NEGOTIATE version=1 features=unknown require_features=true", ctx)
	if resp != "ERROR: UNSUPPORTED_FEATURE class=validation retryable=false features=unknown" {
		t.Fatalf("unexpected response: %s", resp)
	}
	if len(ctx.EnabledFeatures()) != 0 {
		t.Fatalf("failed negotiation changed context: %v", ctx.EnabledFeatures())
	}
}

func TestNegotiateValidatesVersionAndContext(t *testing.T) {
	ch, _ := newTestHandler(t)
	tests := map[string]string{
		"NEGOTIATE":           "ERROR: missing_protocol_version",
		"NEGOTIATE version=x": "ERROR: invalid_protocol_version",
		"NEGOTIATE version=2": "ERROR: UNSUPPORTED_PROTOCOL_VERSION",
	}
	for command, prefix := range tests {
		if resp := ch.HandleCommand(command, NewClientContext("", 0)); !strings.HasPrefix(resp, prefix) {
			t.Fatalf("%s returned %s", command, resp)
		}
	}
	if resp := ch.HandleCommand("NEGOTIATE version=1", nil); !strings.HasPrefix(resp, "ERROR: negotiation_context_required") {
		t.Fatalf("nil context returned %s", resp)
	}
}

func TestNegotiatedSyntaxErrorsAreClassified(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	ch.HandleCommand("NEGOTIATE version=1 features=structured_errors_v1", ctx)
	resp := ch.HandleCommand("CONSUME topic=t", ctx)
	if resp != "ERROR: invalid_consume_syntax class=validation retryable=false" {
		t.Fatalf("unexpected syntax error: %s", resp)
	}
}
