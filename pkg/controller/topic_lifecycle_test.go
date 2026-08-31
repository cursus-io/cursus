package controller

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestDeleteIfExistsAndInternalTopicProtection(t *testing.T) {
	handler, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	require.Equal(t, "OK topic=missing deleted=false", handler.HandleCommand("DELETE topic=missing if_exists=true", ctx))
	require.Contains(t, handler.HandleCommand("DELETE topic=missing", ctx), "topic_not_found")
	require.Contains(t, handler.HandleCommand("DELETE topic=missing if_exists=maybe", ctx), "invalid_if_exists")
	require.Contains(t, handler.HandleCommand("DELETE topic="+config.ConsumerOffsetsTopicName+" if_exists=true", ctx), "internal_topic_delete_forbidden")
}

func TestDeleteFailsWithActiveGroupAndClearsInactiveOffsets(t *testing.T) {
	handler, _, coordinator := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	require.NoError(t, coordinator.RegisterGroup("orders", "workers", 1))
	require.NoError(t, coordinator.CommitOffset("workers", "orders", 0, 17))
	_, err := coordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	blocked := handler.HandleCommand("DELETE topic=orders", ctx)
	require.Contains(t, blocked, "topic_delete_blocked")
	require.NotNil(t, handler.TopicManager.GetTopic("orders"))

	require.NoError(t, coordinator.RemoveConsumer("workers", "member-1"))
	require.Equal(t, "OK topic=orders deleted=true", handler.HandleCommand("DELETE topic=orders", ctx))
	require.Nil(t, coordinator.GetGroup("workers"))

	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	_, found := coordinator.GetOffset("workers", "orders", 0)
	require.False(t, found, "recreating a topic must not revive a deleted group's offset")
}

func TestDeleteFailsWithActiveTransactionAndPrunesTerminalReferences(t *testing.T) {
	handler, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateOpen,
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
	})

	require.Contains(t, handler.HandleCommand("DELETE topic=orders", ctx), "topic_delete_blocked")
	require.NotNil(t, handler.TopicManager.GetTopic("orders"))

	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}, {Topic: "audit", Partition: 0}},
	})
	require.Equal(t, "OK topic=orders deleted=true", handler.HandleCommand("DELETE topic=orders", ctx))
	state := handler.TxnManager.ExportState()["tx-orders"]
	require.Equal(t, []transaction.MessageOperation{{Topic: "audit", Partition: 0}}, state.Messages)
}

func TestDeleteRewritesStandaloneTransactionJournalWithoutTopicReferences(t *testing.T) {
	handler, manager := newTestHandler(t)
	journalPath := filepath.Join(t.TempDir(), "transactions.journal")
	require.NoError(t, handler.ConfigureTransactionJournal(journalPath))
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}, {Topic: "audit", Partition: 0}},
	})
	require.NoError(t, handler.syncTransactionState("tx-orders"))
	require.Equal(t, "OK topic=orders deleted=true", handler.HandleCommand("DELETE topic=orders", ctx))

	reloaded := NewCommandHandler(manager, handler.Config, nil, nil, nil)
	t.Cleanup(func() { _ = reloaded.Close() })
	require.NoError(t, reloaded.ConfigureTransactionJournal(journalPath))
	state := reloaded.TxnManager.ExportState()["tx-orders"]
	require.Equal(t, []transaction.MessageOperation{{Topic: "audit", Partition: 0}}, state.Messages)
}

func TestDeleteDoesNotPruneDependenciesBeforeLogicalCommit(t *testing.T) {
	handler, _, groupCoordinator := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	require.NoError(t, groupCoordinator.CommitOffset("workers", "orders", 0, 17))
	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
	})
	handler.TopicManager.SetDeleteHook(func(string) error { return errors.New("injected pre-commit failure") })

	response := handler.HandleCommand("DELETE topic=orders", ctx)
	require.Contains(t, response, "delete_topic_failed")
	require.NotNil(t, handler.TopicManager.GetTopic("orders"))
	require.NotNil(t, groupCoordinator.GetGroup("workers"))
	_, found := groupCoordinator.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Len(t, handler.TxnManager.ExportState()["tx-orders"].Messages, 1)
}

func TestCreateBlocksStaleLifecycleReferencesUntilIdempotentDeleteCleanup(t *testing.T) {
	handler, _, groupCoordinator := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	require.NoError(t, groupCoordinator.CommitOffset("workers", "orders", 0, 17))

	deleted, err := handler.TopicManager.DeleteTopicDurable("orders")
	require.NoError(t, err)
	require.True(t, deleted)
	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "lifecycle cleanup is pending")
	require.Equal(t, "OK topic=orders deleted=false", handler.HandleCommand("DELETE topic=orders if_exists=true", ctx))
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	_, found := groupCoordinator.GetOffset("workers", "orders", 0)
	require.False(t, found)
}
