package controller

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestStandaloneTruncateDependencyFailureKeepsPublicationFenced(t *testing.T) {
	handler, manager, groups := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK ")
	require.NoError(t, groups.RegisterGroup("orders", "workers", 1))
	require.NoError(t, groups.CommitOffset("workers", "orders", 0, 17))
	groups.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
		return errors.New("injected durable group cleanup failure")
	})

	response := handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx)
	require.Contains(t, response, "truncated=true")
	require.Contains(t, response, "cleanup_pending=true")
	require.True(t, manager.IsTruncationPending("orders"))
	require.Nil(t, manager.GetTopic("orders"))
	require.Error(t, manager.MetadataReadinessError())
	_, err := os.Stat(filepath.Join(handler.Config.LogDir, "orders", ".cursus_lifecycle_epoch"))
	require.ErrorIs(t, err, os.ErrNotExist)

	groups.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return nil })
	response = handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx)
	require.NotContains(t, response, "cleanup_pending")
	require.Contains(t, response, "revision=2 lifecycle_epoch=2")
	require.False(t, manager.IsTruncationPending("orders"))
	require.NotNil(t, manager.GetTopic("orders"))
	require.Nil(t, groups.GetGroup("workers"))
}

func TestStandaloneTruncateJournalFailureKeepsPublicationFenced(t *testing.T) {
	handler, manager := newTestHandler(t)
	ctx := NewClientContext("", 0)
	journalPath := filepath.Join(t.TempDir(), "transactions.journal")
	require.NoError(t, handler.ConfigureTransactionJournal(journalPath))
	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK ")
	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx", Producer: "producer", Revision: 1, State: transaction.StateCommitted,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []transaction.MessageOperation{{Topic: "orders"}, {Topic: "audit"}},
	})
	require.NoError(t, handler.syncTransactionState("tx"))
	// Keep the old durable journal intact while making atomic replacement fail.
	require.NoError(t, os.Rename(journalPath, journalPath+".saved"))
	require.NoError(t, os.Mkdir(journalPath, 0o700))
	response := handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx)
	require.Contains(t, response, "cleanup_pending=true")
	require.True(t, manager.IsTruncationPending("orders"))
	require.Nil(t, manager.GetTopic("orders"))
	require.Len(t, handler.TxnManager.ExportState()["tx"].Messages, 2)

	require.NoError(t, os.Remove(journalPath))
	require.NoError(t, os.Rename(journalPath+".saved", journalPath))
	// Startup recovery must also keep the fence if storage cleanup fails.
	manager.SetDeleteHook(func(string) error { return errors.New("injected recovery storage failure") })
	require.ErrorContains(t, handler.RecoverPendingTruncations(), "injected recovery storage failure")
	require.True(t, manager.IsTruncationPending("orders"))
	require.Len(t, handler.TxnManager.ExportState()["tx"].Messages, 2)
	manager.SetDeleteHook(nil)
	require.NoError(t, handler.RecoverPendingTruncations())
	require.False(t, manager.IsTruncationPending("orders"))
	require.Equal(t, []transaction.MessageOperation{{Topic: "audit"}}, handler.TxnManager.ExportState()["tx"].Messages)
	reloaded := NewCommandHandler(manager, handler.Config, nil, nil, nil)
	t.Cleanup(func() { _ = reloaded.Close() })
	require.NoError(t, reloaded.ConfigureTransactionJournal(journalPath))
	require.Equal(t, []transaction.MessageOperation{{Topic: "audit"}}, reloaded.TxnManager.ExportState()["tx"].Messages)
}
