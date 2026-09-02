package sdk

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestBrokerErrorFromWirePreservesStructuredFields(t *testing.T) {
	err := brokerErrorFromWire(&wire.BrokerError{
		Code: "offset_regression", Class: wire.ErrorClassConflict,
		Fields: map[string]string{"reason": "current offset is 10", "current": "10", "requested": "9"},
	})
	if err.Code != "offset_regression" || err.Class != ErrorClassConflict || err.Retryable {
		t.Fatalf("unexpected error: %+v", err)
	}
	if err.Fields["reason"] != "current offset is 10" || err.Fields["current"] != "10" {
		t.Fatalf("unexpected fields: %+v", err.Fields)
	}
}

func TestBrokerErrorMatchesExistingSentinels(t *testing.T) {
	tests := []struct {
		brokerErr *BrokerError
		target    error
	}{
		{&BrokerError{Code: "topic_not_found", Class: ErrorClassNotFound}, ErrTopicNotFound},
		{&BrokerError{Code: "partition_not_found", Class: ErrorClassNotFound}, ErrInvalidPartition},
	}
	for _, test := range tests {
		if !errors.Is(test.brokerErr, test.target) {
			t.Fatalf("%s did not match %v", test.brokerErr.Code, test.target)
		}
	}
}
