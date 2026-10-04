package sdk

import "testing"

func TestProducerRetryClassifierUsesStructuredRoutingMetadata(t *testing.T) {
	err := &BrokerError{Code: "NOT_LEADER", Class: ErrorClassRouting, Retryable: true}
	if isNonRetryableProducerError(err) {
		t.Fatal("retryable routing error was classified as non-retryable")
	}
}

func TestProducerRetryClassifierUsesStructuredMetadata(t *testing.T) {
	err := &BrokerError{Code: "broker_error", Class: ErrorClassInternal, Retryable: false}
	if !isNonRetryableProducerError(err) {
		t.Fatal("non-retryable structured error was not honored")
	}
}

func TestBrokerErrorUsesReasonFieldWhenMessageIsEmpty(t *testing.T) {
	err := &BrokerError{
		Code:   "broker_error",
		Class:  ErrorClassInternal,
		Fields: map[string]string{"reason": "failed to append batch locally"},
	}
	want := "broker error broker_error (internal): failed to append batch locally"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
