package controller

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRetryableTransactionStateLagUsesStructuredCodes(t *testing.T) {
	for _, response := range []string{
		"ERROR: transaction_not_found transactional_id=tx-1",
		"ERROR: transaction_not_open transactional_id=tx-1 state=aborted",
		"ERROR: transaction_not_committing transactional_id=tx-1 state=open",
		"ERROR: transaction_record_not_staged transactional_id=tx-1",
		"ERROR: transaction_marker_partition_not_touched transactional_id=tx-1",
		"ERROR: transaction_not_abortable transactional_id=tx-1",
		"ERROR: producer_fenced transactional_id=tx-1",
		"ERROR: broker_error reason=\"replication failed: replica broker-3 rejected append: ERROR: transaction_not_committing transactional_id=tx-1 state=open\"",
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
		"ERROR: broker_error reason=transaction_not_committing",
	} {
		if isRetryableTransactionStateLag(response) {
			t.Fatalf("non-state-lag response was retryable: %s", response)
		}
	}
}

func TestReplicatedTransactionStateWaitsForRequiredRevision(t *testing.T) {
	ch := &CommandHandler{TxnManager: transaction.NewManager()}
	messages := []types.Message{{TransactionalID: "tx-1", ProducerID: "producer-1", Epoch: 2}}
	applied := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		applied <- ch.TxnManager.ApplyReplicatedSnapshot(&transaction.Snapshot{
			ID: "tx-1", Mode: transaction.ModeProcessingV1, Producer: "producer-1", Epoch: 2,
			CoordinatorEpoch: 1, Revision: 4, State: transaction.StateOpen,
			Participants: []transaction.Participant{{Topic: "orders", Partition: 0}},
			CreatedAt:    time.Now(), UpdatedAt: time.Now(),
		})
	}()

	started := time.Now()
	require.Empty(t, ch.waitForReplicatedTransactionState(messages, 4, 200*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(started), 15*time.Millisecond)
	require.NoError(t, <-applied)
}

func TestReplicatedTransactionStateFenceRejectsNewerProducerEpoch(t *testing.T) {
	ch := &CommandHandler{TxnManager: transaction.NewManager()}
	now := time.Now()
	require.NoError(t, ch.TxnManager.ApplyReplicatedSnapshot(&transaction.Snapshot{
		ID: "tx-1", Mode: transaction.ModeProcessingV1, Producer: "producer-1", Epoch: 3,
		CoordinatorEpoch: 1, Revision: 1, State: transaction.StateOpen, CreatedAt: now, UpdatedAt: now,
	}))

	response := ch.waitForReplicatedTransactionState(
		[]types.Message{{TransactionalID: "tx-1", ProducerID: "producer-1", Epoch: 2}},
		1,
		50*time.Millisecond,
	)
	require.Contains(t, response, "producer_fenced")
}

func TestReplicatedProcessingRecordAcceptsAppliedSuccessorStates(t *testing.T) {
	for _, state := range []transaction.State{
		transaction.StateOpen,
		transaction.StatePrepareCommit,
		transaction.StateCommitting,
		transaction.StateCommitted,
		transaction.StatePrepareAbort,
		transaction.StateAborted,
	} {
		t.Run(string(state), func(t *testing.T) {
			tx := &transaction.Transaction{
				ID: "tx-1", Mode: transaction.ModeProcessingV1, Producer: "producer-1", Epoch: 2, State: state,
				Participants: []transaction.Participant{{Topic: "orders", Partition: 0}},
			}
			require.Empty(t, validateReplicatedProcessingRecord(tx, "orders", 0))
		})
	}
}

func TestLeaderProcessingRecordStillRejectsTerminalState(t *testing.T) {
	ch := &CommandHandler{}
	tx := &transaction.Transaction{
		ID: "tx-1", Mode: transaction.ModeProcessingV1, Producer: "producer-1", Epoch: 2, State: transaction.StateAborted,
		Participants: []transaction.Participant{{Topic: "orders", Partition: 0}},
	}
	msg := &types.Message{TransactionalID: "tx-1", ProducerID: "producer-1", Epoch: 2}
	require.Contains(t, ch.validateTransactionRecordPublish(tx, "orders", 0, msg), "transaction_not_open")
}

func TestTransactionRecordPublishAcquiresPartitionOwnerStateFence(t *testing.T) {
	ch := &CommandHandler{}
	stateLock := ch.transactionStateLock("tx-1")
	stateLock.Lock()

	acquired := make(chan func(), 1)
	go func() {
		acquired <- ch.lockTransactionPublishState(
			NewInternalClientContext("", 0),
			&types.Message{TransactionalID: "tx-1"},
		)
	}()

	select {
	case release := <-acquired:
		release()
		t.Fatal("publish acquired the transaction fence before the owner released it")
	case <-time.After(20 * time.Millisecond):
	}
	stateLock.Unlock()
	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("publish did not acquire the released transaction fence")
	}
}

func TestCoordinatorLocalTransactionPublishDoesNotRelockStateFence(t *testing.T) {
	ch := &CommandHandler{}
	ctx := NewInternalClientContext("", 0)
	ctx.transactionStateLockHeld = true
	stateLock := ch.transactionStateLock("tx-1")
	stateLock.Lock()
	defer stateLock.Unlock()

	completed := make(chan struct{})
	go func() {
		ch.lockTransactionPublishState(ctx, &types.Message{TransactionalID: "tx-1"})()
		close(completed)
	}()

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("coordinator-local publish attempted to reacquire its transaction fence")
	}
}

func TestTransactionMarkerPublishDoesNotAcquireRecordFence(t *testing.T) {
	ch := &CommandHandler{}
	stateLock := ch.transactionStateLock("tx-1")
	stateLock.Lock()
	defer stateLock.Unlock()

	completed := make(chan struct{})
	go func() {
		ch.lockTransactionPublishState(
			NewInternalClientContext("", 0),
			&types.Message{TransactionalID: "tx-1", TransactionMarker: types.TransactionMarkerCommit},
		)()
		close(completed)
	}()

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("transaction marker attempted to acquire the open-record fence")
	}
}
