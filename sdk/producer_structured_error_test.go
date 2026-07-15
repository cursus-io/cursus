package sdk

import (
	"errors"
	"testing"
)

func TestParseAckResponseReturnsTypedBrokerError(t *testing.T) {
	producer := &Producer{}
	_, err := producer.parseAckResponse([]byte("ERROR: NOT_LEADER class=routing retryable=true LEADER_IS broker-2:9000"))
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("expected BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != "NOT_LEADER" || !brokerErr.Retryable {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
	if isNonRetryableProducerError(err) {
		t.Fatal("retryable routing error was classified as non-retryable")
	}
}

func TestProducerRetryClassifierUsesStructuredMetadata(t *testing.T) {
	err, ok := ParseBrokerError("ERROR: broker_error class=internal retryable=false reason=failed")
	if !ok || !isNonRetryableProducerError(err) {
		t.Fatal("non-retryable structured error was not honored")
	}
}
