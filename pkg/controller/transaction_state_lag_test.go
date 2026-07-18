package controller

import "testing"

func TestRetryableTransactionStateLagUsesStructuredCodes(t *testing.T) {
	for _, response := range []string{
		"ERROR: transaction_not_found transactional_id=tx-1",
		"ERROR: transaction_not_committing transactional_id=tx-1 state=open",
		"ERROR: transaction_record_not_staged transactional_id=tx-1",
		"ERROR: transaction_marker_partition_not_touched transactional_id=tx-1",
		"ERROR: transaction_not_abortable transactional_id=tx-1",
		"ERROR: producer_fenced transactional_id=tx-1",
	} {
		if !isRetryableTransactionStateLag(response) {
			t.Fatalf("state-lag response was not retryable: %s", response)
		}
	}
	for _, response := range []string{
		"OK",
		"ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=t",
		"ERROR: invalid_transaction_state state=committed",
		"prefix transaction_not_committing",
	} {
		if isRetryableTransactionStateLag(response) {
			t.Fatalf("non-state-lag response was retryable: %s", response)
		}
	}
}
