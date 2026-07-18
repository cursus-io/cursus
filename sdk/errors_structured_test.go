package sdk

import (
	"errors"
	"testing"
)

func TestParseBrokerErrorPreservesStructuredFields(t *testing.T) {
	err, ok := ParseBrokerError(`ERROR: offset_regression class=conflict retryable=false reason="current offset is 10" current=10 requested=9`)
	if !ok {
		t.Fatal("response was not recognized")
	}
	if err.Code != "offset_regression" || err.Class != ErrorClassConflict || err.Retryable {
		t.Fatalf("unexpected error: %+v", err)
	}
	if err.Fields["reason"] != "current offset is 10" || err.Fields["current"] != "10" {
		t.Fatalf("unexpected fields: %+v", err.Fields)
	}
}

func TestParseBrokerErrorClassifiesLegacyResponse(t *testing.T) {
	err, ok := ParseBrokerError("ERROR NOT_LEADER LEADER_IS broker-2:9000")
	if !ok || err.Class != ErrorClassRouting || !err.Retryable {
		t.Fatalf("unexpected error: %+v", err)
	}
	if !errors.Is(err, ErrNotLeader) {
		t.Fatal("typed broker error does not match ErrNotLeader")
	}
}

func TestBrokerErrorMatchesExistingSentinels(t *testing.T) {
	tests := []struct {
		response string
		target   error
	}{
		{"ERROR: topic_not_found topic=orders", ErrTopicNotFound},
		{"ERROR: partition_not_found partition=99", ErrInvalidPartition},
	}
	for _, test := range tests {
		brokerErr, ok := ParseBrokerError(test.response)
		if !ok || !errors.Is(brokerErr, test.target) {
			t.Fatalf("%q did not match %v", test.response, test.target)
		}
	}
}
