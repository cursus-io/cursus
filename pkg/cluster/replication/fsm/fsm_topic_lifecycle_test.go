package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type failingTopicCleanupProvider struct {
	MockHandlerProvider
}

func (*failingTopicCleanupProvider) RemoveTopicStorage(string) error {
	return errors.New("injected cleanup failure")
}

func TestBrokerFSMTopicDeleteCleansLifecycleStateAndIsExplicitlyIdempotent(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, manager)
	transactions := transaction.NewManager()
	fsm := NewBrokerFSM(manager, groupCoordinator)
	fsm.SetTransactionManager(transactions)
	registerActiveBroker(t, fsm, "broker-1")

	create, err := json.Marshal(TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	_, err = groupCoordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateOpen,
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
	})
	blocked := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 2})
	blockedErr, ok := blocked.(error)
	require.True(t, ok)
	require.True(t, errors.Is(blockedErr, topic.ErrTopicDeleteBlocked))
	require.NotNil(t, manager.GetTopic("orders"))

	require.NoError(t, groupCoordinator.RemoveConsumer("workers", "member-1"))
	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateCommitted,
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
	})
	fsm.mu.Lock()
	fsm.producerState["orders"] = map[int]map[string]ProducerSequence{0: {"producer": {Seq: 3}}}
	fsm.mu.Unlock()

	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 3}))
	require.Nil(t, manager.GetTopic("orders"))
	require.Nil(t, groupCoordinator.GetGroup("workers"))
	require.Empty(t, transactions.ExportState()["tx-orders"].Messages)
	fsm.mu.RLock()
	_, producerStateFound := fsm.producerState["orders"]
	fsm.mu.RUnlock()
	require.False(t, producerStateFound)

	result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders","if_exists":true}`), Index: 4})
	require.Equal(t, topic.DeleteResult{Deleted: false}, result)
	internal := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"__consumer_offsets","if_exists":true}`), Index: 5})
	require.Error(t, internal.(error))

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	var encoded bytes.Buffer
	sink := &MockSnapshotSink{Writer: &encoded}
	require.NoError(t, snapshot.Persist(sink))

	restoredManager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	restoredCoordinator := coordinator.NewCoordinator(context.Background(), cfg, restoredManager)
	restoredTransactions := transaction.NewManager()
	restored := NewBrokerFSM(restoredManager, restoredCoordinator)
	restored.SetTransactionManager(restoredTransactions)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	require.Nil(t, restoredManager.GetTopic("orders"))
	require.Nil(t, restoredCoordinator.GetGroup("workers"))
	require.Empty(t, restoredTransactions.ExportState()["tx-orders"].Messages)
}

func TestBrokerFSMTopicDeleteReportsCommittedDeletionWhenLocalCleanupIsPending(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &failingTopicCleanupProvider{}, nil)
	fsm := NewBrokerFSM(manager, nil)
	registerActiveBroker(t, fsm, "broker-1")

	create, err := json.Marshal(TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))

	result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 2})
	require.Equal(t, topic.DeleteResult{Deleted: true, CleanupPending: true}, result)
	require.Nil(t, manager.GetTopic("orders"))
}

func TestBrokerFSMCreateWaitsForStaleLifecycleCleanup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, manager)
	transactions := transaction.NewManager()
	fsm := NewBrokerFSM(manager, groupCoordinator)
	fsm.SetTransactionManager(transactions)
	registerActiveBroker(t, fsm, "broker-1")

	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", State: transaction.StateCommitted,
		Messages: []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
	})
	create, err := json.Marshal(TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()})
	require.NoError(t, err)
	result := fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2})
	require.ErrorContains(t, result.(error), "lifecycle cleanup is pending")

	require.Equal(t, topic.DeleteResult{Deleted: false}, fsm.Apply(&raft.Log{
		Data: []byte(`TOPIC_DELETE:{"topic":"orders","if_exists":true}`), Index: 3,
	}))
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 4}))
}
