package controller

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func stageOperationalTransaction(t *testing.T, ch *CommandHandler, name, topicName string, stream bool) (string, string) {
	t.Helper()
	ctx := NewClientContext("", 0)
	producer, epoch := initTransactionSession(t, ch, ctx, name)
	require.Contains(t, ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=%s producerId=%s epoch=%s", name, producer, epoch), ctx), "OK ")
	command := fmt.Sprintf("TXN_PUBLISH transactional_id=%s topic=%s partition=0 producerId=%s seqNum=1 epoch=%s message=pending", name, topicName, producer, epoch)
	if stream {
		command = fmt.Sprintf("TXN_APPEND_STREAM transactional_id=%s topic=%s key=run expected_version=1 producerId=%s seqNum=1 epoch=%s message=pending", name, topicName, producer, epoch)
	}
	require.Contains(t, ch.HandleCommand(command, ctx), "OK ")
	return producer, epoch
}

func TestProducerReinitializationResolvesOldRecordsAndStreamReservations(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			ch, tm, _, _ := newDiskBackedTransactionHandler(t)
			require.NoError(t, ch.ConfigureTransactionJournal(filepath.Join(t.TempDir(), "transactions.journal")))
			require.NoError(t, tm.CreateTopic("reinitialize", 1, false, stream))
			producer, epoch := stageOperationalTransaction(t, ch, "reinitialize-txn", "reinitialize", stream)
			ctx := NewClientContext("", 0)
			nextProducer, nextEpoch := initTransactionSession(t, ch, ctx, "reinitialize-txn")
			require.Equal(t, producer, nextProducer)
			require.Equal(t, "1", nextEpoch)
			require.Equal(t, "0", epoch)
			_, reserved := ch.TxnManager.StreamReservation("reinitialize", "run")
			require.False(t, reserved)
			if stream {
				require.Contains(t, ch.HandleCommand("APPEND_STREAM topic=reinitialize key=run version=1 message=after-restart", ctx), "OK ")
			} else {
				require.Contains(t, ch.HandleCommand("PUBLISH topic=reinitialize partition=0 producerId=normal seqNum=1 epoch=0 message=after-restart", ctx), `"status":"OK"`)
			}
			require.Equal(t, []string{"after-restart"}, readCommittedPayloads(t, tm, "reinitialize"))
			p, err := tm.GetTopic("reinitialize").GetPartition(0)
			require.NoError(t, err)
			require.Equal(t, p.GetHWM(), p.LastStableOffset())
		})
	}
}

func TestFinalTransactionDecisionWaitsForLocalApplication(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("decision-lag", 1, false, false))
	producer, _ := stageOperationalTransaction(t, ch, "decision-lag", "decision-lag", false)
	_, err := ch.TxnManager.PrepareAbort("decision-lag", producer, 0)
	require.NoError(t, err)
	decision, err := ch.TxnManager.BuildAbortedSnapshot("decision-lag", producer, 0)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- ch.waitForTransactionDecision(decision) }()
	select {
	case err := <-done:
		t.Fatalf("returned before the local decision was applied: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.NoError(t, ch.TxnManager.ApplyReplicatedSnapshot(decision))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("did not observe the locally applied decision")
	}
}

func TestProducerReinitializationRetriesPersistedAbortAfterSyncFailure(t *testing.T) {
	for _, failStage := range []string{"prepare-abort", "new-epoch"} {
		t.Run(failStage, func(t *testing.T) {
			ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
			journal := filepath.Join(t.TempDir(), "transactions.journal")
			require.NoError(t, ch.ConfigureTransactionJournal(journal))
			require.NoError(t, tm.CreateTopic("retry-init", 1, false, false))
			stageOperationalTransaction(t, ch, "retry-init-txn", "retry-init", false)
			ch.transactionStateSyncHook = func(id string) error {
				snap, ok := ch.TxnManager.Snapshot(id)
				require.True(t, ok)
				if failStage == "prepare-abort" && snap.State == transaction.StatePrepareAbort || failStage == "new-epoch" && snap.Epoch == 1 {
					return errors.New("injected sync failure")
				}
				return ch.txnJournal.Append(snap)
			}
			response := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=retry-init-txn", NewClientContext("", 0))
			require.Contains(t, response, "injected sync failure")
			current, err := ch.TxnManager.Status("retry-init-txn")
			require.NoError(t, err)
			require.Zero(t, current.Epoch, "failed initialization must not fence the recoverable old epoch")
			p, err := tm.GetTopic("retry-init").GetPartition(0)
			require.NoError(t, err)
			if failStage == "prepare-abort" {
				require.Equal(t, transaction.StatePrepareAbort, current.State)
				require.Equal(t, uint64(1), p.NextOffset(), "no marker before durable preparation")
			} else {
				require.Equal(t, transaction.StateAborted, current.State, "rollback must preserve completed abort")
			}
			restarted := NewCommandHandler(tm, ch.Config, coord, nil, nil)
			t.Cleanup(func() { _ = restarted.Close() })
			require.NoError(t, restarted.ConfigureTransactionJournal(journal))
			_, epoch := initTransactionSession(t, restarted, NewClientContext("", 0), "retry-init-txn")
			require.Equal(t, "1", epoch)
			require.Equal(t, uint64(2), p.NextOffset(), "retry must append exactly one abort marker")
			require.Equal(t, p.GetHWM(), p.LastStableOffset())
		})
	}
}

func TestTopicLifecycleBlocksActiveV1Transactions(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, state := range []transaction.State{transaction.StateOpen, transaction.StatePrepareCommit, transaction.StatePrepareAbort} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, state), func(t *testing.T) {
				ch, tm, _, _ := newDiskBackedTransactionHandler(t)
				require.NoError(t, tm.CreateTopic("active-topic", 1, false, stream))
				producer, epoch := stageOperationalTransaction(t, ch, "active-txn", "active-topic", stream)
				ep, err := strconv.ParseInt(epoch, 10, 64)
				require.NoError(t, err)
				if state == transaction.StatePrepareCommit {
					_, err = ch.TxnManager.PrepareCommit("active-txn", producer, ep)
				} else if state == transaction.StatePrepareAbort {
					_, err = ch.TxnManager.PrepareAbort("active-txn", producer, ep)
				}
				require.NoError(t, err)
				ctx := NewClientContext("", 0)
				require.Contains(t, ch.HandleCommand("DELETE topic=active-topic", ctx), "topic_delete_blocked")
				require.Contains(t, ch.HandleCommand("TRUNCATE topic=active-topic expected_revision=1", ctx), "topic_truncate_blocked")
				p, err := tm.GetTopic("active-topic").GetPartition(0)
				require.NoError(t, err)
				require.Equal(t, uint64(1), p.NextOffset(), "rejected lifecycle operations must preserve staged data")
			})
		}
	}
}

func TestTransactionMonitorContinuesTimeoutsAfterPreparedFailure(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	ch.Config.TransactionTimeoutMS = 50
	for _, name := range []string{"broken", "healthy"} {
		require.NoError(t, tm.CreateTopic(name, 1, false, false))
		producer, epoch := stageOperationalTransaction(t, ch, name, name, false)
		if name == "broken" {
			ep, err := strconv.ParseInt(epoch, 10, 64)
			require.NoError(t, err)
			_, err = ch.TxnManager.PrepareCommit(name, producer, ep)
			require.NoError(t, err)
		}
	}
	// Keep one durable-state backend unavailable throughout the monitor run.
	ch.transactionStateSyncHook = func(id string) error {
		if id == "broken" {
			return errors.New("injected sync failure")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch.StartTransactionTimeoutMonitor(ctx)
	require.Eventually(t, func() bool {
		tx, err := ch.TxnManager.Status("healthy")
		return err == nil && tx.State == transaction.StateAborted
	}, 3*time.Second, 10*time.Millisecond)
	broken, err := ch.TxnManager.Status("broken")
	require.NoError(t, err)
	require.Equal(t, transaction.StatePrepareCommit, broken.State)
	p, err := tm.GetTopic("broken").GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.NextOffset(), "failed preparation sync must not append a commit marker")
}

func TestTransactionRecoveryProcessesRemainingBatchAfterFirstFailure(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	for _, name := range []string{"first", "second"} {
		require.NoError(t, tm.CreateTopic(name, 1, false, false))
		producer, epoch := stageOperationalTransaction(t, ch, name, name, false)
		ep, err := strconv.ParseInt(epoch, 10, 64)
		require.NoError(t, err)
		_, err = ch.TxnManager.PrepareCommit(name, producer, ep)
		require.NoError(t, err)
	}
	failedID := ""
	ch.transactionStateSyncHook = func(id string) error {
		if failedID == "" {
			failedID = id
		}
		if id == failedID {
			return errors.New("injected first recovery failure")
		}
		return nil
	}
	require.ErrorContains(t, ch.RecoverPreparedTransactions(), "injected first recovery failure")
	for _, name := range []string{"first", "second"} {
		tx, err := ch.TxnManager.Status(name)
		require.NoError(t, err)
		if name == failedID {
			require.Equal(t, transaction.StatePrepareCommit, tx.State)
		} else {
			require.Equal(t, transaction.StateCommitted, tx.State, "an earlier failure must not skip the rest of the batch")
			require.Equal(t, []string{"pending"}, readCommittedPayloads(t, tm, name))
		}
	}
}

func TestTransactionTimeoutProcessesRemainingBatchAfterFirstFailure(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	for _, name := range []string{"first", "second"} {
		require.NoError(t, tm.CreateTopic(name, 1, false, false))
		stageOperationalTransaction(t, ch, name, name, false)
	}
	failedID := ""
	ch.transactionStateSyncHook = func(id string) error {
		if failedID == "" {
			failedID = id
		}
		if id == failedID {
			return errors.New("injected first timeout failure")
		}
		return nil
	}
	require.ErrorContains(t, ch.AbortTimedOutTransactions(time.Now().Add(time.Hour)), "injected first timeout failure")
	for _, name := range []string{"first", "second"} {
		tx, err := ch.TxnManager.Status(name)
		require.NoError(t, err)
		if name == failedID {
			require.Equal(t, transaction.StatePrepareAbort, tx.State)
		} else {
			require.Equal(t, transaction.StateAborted, tx.State)
		}
	}
}
