package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryableInternalTransactionPublishResponseUsesStructuredCodes(t *testing.T) {
	for _, response := range []string{
		"ERROR: replication_unavailable offset=1 reason=\"replica broker-2 rejected append\"",
		"ERROR: insufficient_in_sync_replicas current=1 required=2",
		"ERROR: transaction_not_found transactional_id=tx-1",
		"ERROR: transaction_not_committing transactional_id=tx-1 state=open",
		"ERROR: transaction_record_not_staged transactional_id=tx-1",
		"ERROR: transaction_marker_partition_not_touched transactional_id=tx-1",
		"ERROR: transaction_not_abortable transactional_id=tx-1",
		"ERROR: producer_fenced transactional_id=tx-1",
		"ERROR: broker_error reason=\"replication failed: replica broker-3 rejected append: ERROR: transaction_not_committing transactional_id=tx-1 state=open\"",
	} {
		if !isRetryableInternalTransactionPublishResponse(response) {
			t.Fatalf("state-lag response was not retryable: %s", response)
		}
	}
	for _, response := range []string{
		"OK",
		"ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=t",
		"ERROR: invalid_transaction_state state=committed",
		"prefix transaction_not_committing",
		"ERROR: broker_error reason=transaction_not_committing",
	} {
		if isRetryableInternalTransactionPublishResponse(response) {
			t.Fatalf("non-state-lag response was retryable: %s", response)
		}
	}
}

func TestInternalTransactionPublishRetriesReplicaAvailability(t *testing.T) {
	handler, manager, executor := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, handler, "orders", []string{"broker-1", "broker-2"})

	executor.mu.Lock()
	executor.replicateErr = errors.New("replica broker-2 is catching up")
	executor.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		result <- handler.publishInternalTransactionCommand("PUBLISH topic=orders acks=all producerId=txn-1 partition=0 seqNum=1 epoch=0 isIdempotent=true internal_txn_publish=true message=value")
	}()

	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("transaction publish did not start replication")
	}
	executor.mu.Lock()
	executor.replicateErr = nil
	executor.mu.Unlock()
	close(executor.barrier)

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(DefaultFSMApplyTimeout + time.Second):
		t.Fatal("transaction publish did not retry replica availability")
	}
}
