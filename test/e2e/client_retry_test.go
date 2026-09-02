package e2e

import "testing"

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
