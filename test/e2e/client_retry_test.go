package e2e

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestRetryClassificationIncludesOnlyIdempotentPublish(t *testing.T) {
	if !isIdempotent("PUBLISH topic=orders producerId=p1 seqNum=1 epoch=1 isIdempotent=true message=value") {
		t.Fatal("idempotent publish must be safe to retry after an ambiguous response")
	}
	if isIdempotent("PUBLISH topic=orders producerId=p1 seqNum=1 epoch=1 message=value") {
		t.Fatal("non-idempotent publish must not be retried after an ambiguous response")
	}
	if !isIdempotent("FETCH_OFFSET topic=orders partition=0 group=workers") {
		t.Fatal("read-only offset fetch must remain retryable")
	}
}

func TestRetryableBrokerErrorsRequireIdempotentCommands(t *testing.T) {
	retryable := &wire.BrokerError{Code: "replication_unavailable", Retryable: true}
	permanent := &wire.BrokerError{Code: "producer_fenced", Retryable: false}
	idempotent := "PUBLISH topic=orders producerId=p1 seqNum=1 epoch=1 isIdempotent=true message=value"
	nonIdempotent := "PUBLISH topic=orders producerId=p1 seqNum=1 epoch=1 message=value"

	if !shouldRetryBrokerError(idempotent, retryable) {
		t.Fatal("retryable broker error must be retried for an idempotent command")
	}
	if shouldRetryBrokerError(nonIdempotent, retryable) {
		t.Fatal("ambiguous non-idempotent command must not be retried")
	}
	if shouldRetryBrokerError(idempotent, permanent) {
		t.Fatal("non-retryable broker error must not be retried")
	}
}

func TestSuccessfulResponseAcceptsWireTextAndStructuredPublishResults(t *testing.T) {
	for _, response := range []string{
		"OK",
		"OK topic=orders",
		`{"status":"OK","last_offset":4}`,
	} {
		if !isSuccessfulResponse(response) {
			t.Fatalf("valid response rejected: %s", response)
		}
	}
	for _, response := range []string{"", "ERROR: failed", `{"status":"ERROR"}`, `{"last_offset":4}`} {
		if isSuccessfulResponse(response) {
			t.Fatalf("invalid response accepted: %s", response)
		}
	}
}
