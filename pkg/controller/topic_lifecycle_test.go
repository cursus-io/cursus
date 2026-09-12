package controller

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
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
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateOpen,
		Messages:  []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})

	require.Contains(t, handler.HandleCommand("DELETE topic=orders", ctx), "topic_delete_blocked")
	require.NotNil(t, handler.TopicManager.GetTopic("orders"))

	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", Producer: "producer", Revision: 2, State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
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
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
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
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
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

func TestStandaloneTruncateRequiresRevisionAndResetsLifecycleState(t *testing.T) {
	handler, _, groupCoordinator := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	create := handler.HandleCommand("CREATE topic=orders partitions=1 retention_hours=168 read_acl=reader", ctx)
	require.Contains(t, create, "revision=1")
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	require.NoError(t, groupCoordinator.CommitOffset("workers", "orders", 0, 17))
	handler.TxnManager.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateCommitted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}, {Topic: "audit", Partition: 0}},
	})

	require.Contains(t, handler.HandleCommand("TRUNCATE topic=orders", ctx), "missing_expected_revision")
	require.Contains(t, handler.HandleCommand("TRUNCATE topic=orders expected_revision=9", ctx), "topic_revision_conflict")
	response := handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx)
	require.Equal(t, "OK topic=orders truncated=true revision=2 lifecycle_epoch=2 leo=0 hwm=0", response)

	definition := handler.TopicManager.GetTopic("orders").Definition()
	require.Equal(t, uint64(2), definition.Revision)
	require.Equal(t, uint64(2), definition.LifecycleEpoch)
	require.Equal(t, 168, definition.Policy.RetentionHours)
	require.Equal(t, []string{"reader"}, definition.Policy.ReadACL)
	require.Nil(t, groupCoordinator.GetGroup("workers"))
	_, found := groupCoordinator.GetOffset("workers", "orders", 0)
	require.False(t, found)
	require.Equal(t, []transaction.MessageOperation{{Topic: "audit", Partition: 0}}, handler.TxnManager.ExportState()["tx-orders"].Messages)
	require.Contains(t, handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx), "topic_revision_conflict")
}

func TestStandaloneTruncateFailsClosedForActiveStateAndInternalTopic(t *testing.T) {
	handler, _, groupCoordinator := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.Contains(t, handler.HandleCommand("TRUNCATE topic="+config.ConsumerOffsetsTopicName+" expected_revision=1", ctx), "internal_topic_truncate_forbidden")
	require.True(t, strings.HasPrefix(handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK "))
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	_, err := groupCoordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	require.Contains(t, handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx), "topic_truncate_blocked")
	require.Equal(t, uint64(1), handler.TopicManager.GetTopic("orders").LifecycleEpoch)
}

func TestDistributedLifecycleSubmissionRejectsActiveRuntimeReferences(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		handler, state, groupCoordinator := newDistributedLifecycleHandler(t)
		ctx := NewClientContext("", 0)
		require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1 replication_factor=1", ctx), "OK topic=orders")
		require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
		_, err := groupCoordinator.AddConsumer("workers", "member-1")
		require.NoError(t, err)

		require.Contains(t, handler.HandleCommand("DELETE topic=orders", ctx), "topic_delete_blocked")
		_, found := state.GetTopicDefinition("orders")
		require.True(t, found, "rejected delete must not enter the Raft FSM")
	})

	t.Run("truncate", func(t *testing.T) {
		handler, state, groupCoordinator := newDistributedLifecycleHandler(t)
		ctx := NewClientContext("", 0)
		require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1 replication_factor=1", ctx), "OK topic=orders")
		require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
		_, err := groupCoordinator.AddConsumer("workers", "member-1")
		require.NoError(t, err)

		require.Contains(t, handler.HandleCommand("TRUNCATE topic=orders expected_revision=1", ctx), "topic_truncate_blocked")
		definition, found := state.GetTopicDefinition("orders")
		require.True(t, found)
		require.Equal(t, uint64(1), definition.LifecycleEpoch, "rejected truncate must not enter the Raft FSM")
	})

	t.Run("recreate", func(t *testing.T) {
		handler, state, groupCoordinator := newDistributedLifecycleHandler(t)
		ctx := NewClientContext("", 0)
		require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1 replication_factor=1", ctx), "OK topic=orders")
		require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
		_, err := groupCoordinator.AddConsumer("workers", "member-1")
		require.NoError(t, err)

		// Model an already-committed delete being replayed after the group runtime
		// was restored. The FSM advances deterministically and records local cleanup
		// as pending; a new CREATE is still rejected at the controller boundary.
		result := state.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 3})
		require.Equal(t, topic.DeleteResult{Deleted: true, CleanupPending: true}, result)
		require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1 replication_factor=1", ctx), "lifecycle cleanup is pending")
		_, found := state.GetTopicDefinition("orders")
		require.False(t, found)
	})
}

func newDistributedLifecycleHandler(t *testing.T) (*CommandHandler, *fsm.BrokerFSM, *coordinator.Coordinator) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, &dummyPublisher{})
	state := fsm.NewBrokerFSM(manager, groupCoordinator)
	broker, err := json.Marshal(fsm.BrokerInfo{
		ID: "broker-1", Addr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9000", Status: "active",
		LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
	})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), broker...), Index: 1}))
	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	raftManager.leaderAddress.Store("127.0.0.1:9001")
	handler := NewCommandHandler(manager, cfg, groupCoordinator, nil, &clusterController.ClusterController{RaftManager: raftManager})
	groupCoordinator.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return nil })
	t.Cleanup(func() { _ = handler.Close() })
	return handler, state, groupCoordinator
}
