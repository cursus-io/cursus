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
