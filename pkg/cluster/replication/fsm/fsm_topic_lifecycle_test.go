package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type failingTopicCleanupProvider struct {
	MockHandlerProvider
}

func (*failingTopicCleanupProvider) RemoveTopicStorage(string) error {
	return errors.New("injected cleanup failure")
}

type recoverableTopicCleanupProvider struct {
	MockHandlerProvider
	fail bool
}

func (p *recoverableTopicCleanupProvider) RemoveTopicStorage(string) error {
	if p.fail {
		return errors.New("injected cleanup failure")
	}
	return nil
}

type recoverableLifecyclePublisher struct {
	fail bool
}

func (p *recoverableLifecyclePublisher) Publish(string, *types.Message) error {
	if p.fail {
		return errors.New("injected lifecycle persistence failure")
	}
	return nil
}

func (*recoverableLifecyclePublisher) CreateTopic(string, int, bool, bool) error {
	return nil
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

	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	_, err = groupCoordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateOpen,
		Messages:  []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	blocked := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 2})
	blockedErr, ok := blocked.(error)
	require.True(t, ok)
	require.True(t, errors.Is(blockedErr, topic.ErrTopicDeleteBlocked))
	require.NotNil(t, manager.GetTopic("orders"))

	require.NoError(t, groupCoordinator.RemoveConsumer("workers", "member-1"))
	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", Producer: "producer", Revision: 2, State: transaction.StateCommitted,
		Messages:  []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
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

	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))

	result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 2})
	require.Equal(t, topic.DeleteResult{Deleted: true, CleanupPending: true}, result)
	require.Nil(t, manager.GetTopic("orders"))
}

func TestBrokerFSMLifecycleDependencyCleanupRunsAfterCommitAndReconciles(t *testing.T) {
	newFSM := func(t *testing.T) (*BrokerFSM, *topic.TopicManager, *coordinator.Coordinator, *recoverableLifecyclePublisher) {
		t.Helper()
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
		t.Cleanup(manager.Stop)
		publisher := &recoverableLifecyclePublisher{}
		groupCoordinator, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, publisher)
		require.NoError(t, err)
		t.Cleanup(groupCoordinator.Stop)
		fsm := NewBrokerFSM(manager, groupCoordinator)
		fsm.SetTransactionManager(transaction.NewManager())
		registerLifecycleBroker(t, fsm, "broker-1", TopicLifecycleProtocolVersion)
		create, err := json.Marshal(testTopicCommand("orders", 1, 1))
		require.NoError(t, err)
		require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2}))
		require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
		return fsm, manager, groupCoordinator, publisher
	}

	t.Run("delete", func(t *testing.T) {
		fsm, manager, groupCoordinator, publisher := newFSM(t)
		publisher.fail = true

		result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 3})
		require.Equal(t, topic.DeleteResult{Deleted: true, CleanupPending: true}, result)
		_, found := fsm.GetTopicDefinition("orders")
		require.False(t, found, "replicated delete must commit before dependency cleanup")
		require.Nil(t, manager.GetTopic("orders"), "the deleted local topic must be fenced")
		require.NotNil(t, groupCoordinator.GetGroup("workers"), "failed cleanup must remain retryable")
		require.Equal(t, TopicMaterializationDelete, fsm.TopicMaterializationIssues()[0].Operation)

		publisher.fail = false
		require.NoError(t, fsm.ReconcileTopicMaterializations())
		require.Nil(t, groupCoordinator.GetGroup("workers"))
		require.Empty(t, fsm.TopicMaterializationIssues())
	})

	t.Run("truncate", func(t *testing.T) {
		fsm, manager, groupCoordinator, publisher := newFSM(t)
		publisher.fail = true

		result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
		truncate := result.(topic.TruncateResult)
		require.True(t, truncate.Truncated)
		require.True(t, truncate.CleanupPending)
		definition, found := fsm.GetTopicDefinition("orders")
		require.True(t, found)
		require.Equal(t, uint64(2), definition.LifecycleEpoch)
		require.Nil(t, manager.GetTopic("orders"), "the old local generation must be fenced")
		require.NotNil(t, groupCoordinator.GetGroup("workers"), "failed cleanup must remain retryable")
		require.Equal(t, TopicMaterializationTruncate, fsm.TopicMaterializationIssues()[0].Operation)

		publisher.fail = false
		require.NoError(t, fsm.ReconcileTopicMaterializations())
		require.Nil(t, groupCoordinator.GetGroup("workers"))
		require.NotNil(t, manager.GetTopic("orders"))
		require.Equal(t, uint64(2), manager.GetTopic("orders").LifecycleEpoch)
		require.Empty(t, fsm.TopicMaterializationIssues())
	})
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
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateCommitted,
		Messages:  []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	result := fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2})
	require.ErrorContains(t, result.(error), "lifecycle cleanup is pending")

	require.Equal(t, topic.DeleteResult{Deleted: false}, fsm.Apply(&raft.Log{
		Data: []byte(`TOPIC_DELETE:{"topic":"orders","if_exists":true}`), Index: 3,
	}))
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 4}))
}

func TestBrokerFSMTopicTruncateResetsStateAndFencesOldLifecycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, manager)
	transactions := transaction.NewManager()
	fsm := NewBrokerFSM(manager, groupCoordinator)
	fsm.SetTransactionManager(transactions)
	registerLifecycleBroker(t, fsm, "broker-1", TopicLifecycleProtocolVersion)

	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2}))

	localTopic := manager.GetTopic("orders")
	require.NotNil(t, localTopic)
	partition, err := localTopic.GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.EnqueueBatchSync([]types.Message{{ProducerID: "producer", SeqNum: 1, Payload: "old-1"}, {ProducerID: "producer", SeqNum: 2, Payload: "old-2"}}))
	partition.AdvanceHWM()
	oldLEO := partition.NextOffset()
	require.Greater(t, oldLEO, uint64(0))
	require.Equal(t, oldLEO, partition.GetHWM())

	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	require.NoError(t, groupCoordinator.CommitOffset("workers", "orders", 0, oldLEO))
	transactions.ApplySnapshot(&transaction.Snapshot{
		ID: "tx-orders", Producer: "producer", Revision: 1, State: transaction.StateCommitted,
		Messages:  []transaction.MessageOperation{{Topic: "orders", Partition: 0}},
		Offsets:   []transaction.OffsetOperation{{Topic: "orders", Group: "workers", Partition: 0, Offset: oldLEO}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	fsm.mu.Lock()
	metadataBefore := *fsm.partitionMetadata["orders-0"]
	metadataBefore.CommittedHWM = oldLEO
	fsm.partitionMetadata["orders-0"].CommittedHWM = oldLEO
	fsm.logs[10] = &ReplicationEntry{Topic: "orders", Partition: 0, Message: types.Message{Payload: "old"}}
	fsm.producerState["orders"] = map[int]map[string]ProducerSequence{0: {"producer": {Epoch: 1, Seq: 2}}}
	fsm.mu.Unlock()

	result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
	truncate, ok := result.(topic.TruncateResult)
	require.True(t, ok, "unexpected truncate result: %#v", result)
	require.True(t, truncate.Truncated)
	require.False(t, truncate.CleanupPending)
	require.Equal(t, uint64(2), truncate.Definition.Revision)
	require.Equal(t, uint64(2), truncate.Definition.LifecycleEpoch)

	definition, found := fsm.GetTopicDefinition("orders")
	require.True(t, found)
	require.Equal(t, uint64(2), definition.Revision)
	require.Equal(t, uint64(2), definition.LifecycleEpoch)
	metadata := fsm.GetPartitionMetadata("orders-0")
	require.NotNil(t, metadata)
	require.Equal(t, uint64(0), metadata.CommittedHWM)
	require.Equal(t, metadataBefore.LeaderEpoch+1, metadata.LeaderEpoch)
	require.Equal(t, uint64(2), metadata.LifecycleEpoch)
	require.Nil(t, groupCoordinator.GetGroup("workers"))
	require.Empty(t, transactions.ExportState()["tx-orders"].Messages)
	require.Empty(t, transactions.ExportState()["tx-orders"].Offsets)
	fsm.mu.RLock()
	_, producerStateFound := fsm.producerState["orders"]
	_, oldLogFound := fsm.logs[10]
	fsm.mu.RUnlock()
	require.False(t, producerStateFound)
	require.False(t, oldLogFound)

	localTopic = manager.GetTopic("orders")
	require.NotNil(t, localTopic)
	require.Equal(t, uint64(2), localTopic.LifecycleEpoch)
	partition, err = localTopic.GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), partition.NextOffset())
	require.Equal(t, uint64(0), partition.GetHWM())

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &encoded}))
	var persisted BrokerFSMState
	require.NoError(t, json.Unmarshal(encoded.Bytes(), &persisted))
	require.Equal(t, SnapshotVersionCurrent, persisted.Version, "truncated lifecycle requires the clean-bootstrap snapshot format")
	restoredCfg := *cfg
	restoredCfg.LogDir = t.TempDir()
	restoredManager := topic.NewTopicManager(&restoredCfg, &MockHandlerProvider{}, nil)
	restored := NewBrokerFSM(restoredManager, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	restoredDefinition, found := restored.GetTopicDefinition("orders")
	require.True(t, found)
	require.Equal(t, uint64(2), restoredDefinition.Revision)
	require.Equal(t, uint64(2), restoredDefinition.LifecycleEpoch)
	require.Equal(t, uint64(2), restored.GetPartitionMetadata("orders-0").LifecycleEpoch)
	restoredTopic := restoredManager.GetTopic("orders")
	require.NotNil(t, restoredTopic)
	restoredPartition, err := restoredTopic.GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), restoredPartition.NextOffset())

	stalePayload, err := json.Marshal(types.MessageCommand{
		Topic: "orders", Partition: 0, LifecycleEpoch: 1,
		Messages: []types.Message{{ProducerID: "producer", SeqNum: 1, Payload: "stale"}},
	})
	require.NoError(t, err)
	staleAck := fsm.Apply(&raft.Log{Data: []byte("MESSAGE:" + string(stalePayload)), Index: 4}).(types.AckResponse)
	require.Equal(t, "ERROR", staleAck.Status)
	require.ErrorContains(t, errors.New(staleAck.ErrorMsg), "stale topic lifecycle epoch")

	missingPayload, err := json.Marshal(types.MessageCommand{
		Topic: "orders", Partition: 0,
		Messages: []types.Message{{ProducerID: "producer", SeqNum: 1, Payload: "missing"}},
	})
	require.NoError(t, err)
	missingAck := fsm.Apply(&raft.Log{Data: []byte("MESSAGE:" + string(missingPayload)), Index: 5}).(types.AckResponse)
	require.Equal(t, "ERROR", missingAck.Status)
	require.ErrorContains(t, errors.New(missingAck.ErrorMsg), "missing topic lifecycle epoch")

	currentPayload, err := json.Marshal(types.MessageCommand{
		Topic: "orders", Partition: 0, LifecycleEpoch: 2,
		Messages: []types.Message{{ProducerID: "producer", SeqNum: 1, Payload: "current"}},
	})
	require.NoError(t, err)
	currentAck := fsm.Apply(&raft.Log{Data: []byte("MESSAGE:" + string(currentPayload)), Index: 6}).(types.AckResponse)
	require.Equal(t, "OK", currentAck.Status)

	staleCommit := fmt.Sprintf(
		`PARTITION_COMMIT:{"topic":"orders","partition":0,"leader":%q,"leader_epoch":%d,"hwm":1,"lifecycle_epoch":1}`,
		metadata.Leader, metadata.LeaderEpoch,
	)
	commitResult := fsm.Apply(&raft.Log{Data: []byte(staleCommit), Index: 7})
	require.ErrorContains(t, commitResult.(error), "stale topic lifecycle epoch")
	currentCommit := fmt.Sprintf(
		`PARTITION_COMMIT:{"topic":"orders","partition":0,"leader":%q,"leader_epoch":%d,"hwm":1,"lifecycle_epoch":2}`,
		metadata.Leader, metadata.LeaderEpoch,
	)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte(currentCommit), Index: 8}))
}

func TestBrokerFSMTopicTruncateRejectsUnsafeClusterAndActiveState(t *testing.T) {
	newFSM := func(t *testing.T, lifecycleProtocol int) (*BrokerFSM, *topic.TopicManager, *coordinator.Coordinator) {
		t.Helper()
		cfg := config.DefaultConfig()
		cfg.EnabledDistribution = true
		cfg.LogDir = t.TempDir()
		manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
		groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, manager)
		fsm := NewBrokerFSM(manager, groupCoordinator)
		registerLifecycleBroker(t, fsm, "broker-1", lifecycleProtocol)
		create, err := json.Marshal(testTopicCommand("orders", 1, 1))
		require.NoError(t, err)
		require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2}))
		return fsm, manager, groupCoordinator
	}

	t.Run("mixed broker capability", func(t *testing.T) {
		fsm, manager, _ := newFSM(t, 0)
		result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
		require.ErrorContains(t, result.(error), "requires lifecycle protocol")
		require.Equal(t, uint64(1), manager.GetTopic("orders").Revision)
		require.Equal(t, uint64(1), manager.GetTopic("orders").LifecycleEpoch)
	})

	t.Run("active group", func(t *testing.T) {
		fsm, manager, groupCoordinator := newFSM(t, TopicLifecycleProtocolVersion)
		require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
		_, err := groupCoordinator.AddConsumer("workers", "member-1")
		require.NoError(t, err)
		result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
		require.Error(t, result.(error))
		require.True(t, errors.Is(result.(error), topic.ErrTopicDeleteBlocked))
		require.Equal(t, uint64(1), manager.GetTopic("orders").Revision)
	})

	t.Run("revision and internal topic", func(t *testing.T) {
		fsm, manager, _ := newFSM(t, TopicLifecycleProtocolVersion)
		conflict := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":9}`), Index: 3})
		require.True(t, errors.Is(conflict.(error), topic.ErrTopicRevisionConflict))
		internal := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"__consumer_offsets","expected_revision":1}`), Index: 4})
		require.Error(t, internal.(error))
		require.Equal(t, uint64(1), manager.GetTopic("orders").Revision)
	})
}

func TestBrokerFSMTopicUpdateSupersedesPendingTruncateMaterialization(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	provider := &recoverableTopicCleanupProvider{}
	manager := topic.NewTopicManager(cfg, provider, nil)
	fsm := NewBrokerFSM(manager, nil)
	registerLifecycleBroker(t, fsm, "broker-1", TopicLifecycleProtocolVersion)

	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2}))

	provider.fail = true
	result := fsm.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
	truncate := result.(topic.TruncateResult)
	require.True(t, truncate.Truncated)
	require.True(t, truncate.CleanupPending)
	require.True(t, manager.IsTruncationPending("orders"))

	provider.fail = false
	policy := topic.DefaultPolicy()
	policy.RetentionHours = 24
	retentionHours := policy.RetentionHours
	updateCommand := testTopicCommand("orders", 1, 1)
	updateCommand.Patch = &topic.DefinitionPatch{RetentionHours: &retentionHours}
	update, err := json.Marshal(updateCommand)
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: []byte("TOPIC:" + string(update)), Index: 4}))

	definition, found := fsm.GetTopicDefinition("orders")
	require.True(t, found)
	require.Equal(t, uint64(3), definition.Revision)
	require.Equal(t, uint64(2), definition.LifecycleEpoch)
	require.Equal(t, 24, definition.Policy.RetentionHours)
	require.False(t, manager.IsTruncationPending("orders"))
	local := manager.GetTopic("orders")
	require.NotNil(t, local)
	require.Equal(t, uint64(3), local.Revision)
	require.Equal(t, uint64(2), local.LifecycleEpoch)
	require.Equal(t, 24, local.Policy.RetentionHours)
	require.Empty(t, fsm.TopicMaterializationIssues())
}

func registerLifecycleBroker(t *testing.T, f *BrokerFSM, id string, lifecycleProtocol int) {
	t.Helper()
	payload, err := json.Marshal(BrokerInfo{
		ID: id, Addr: "127.0.0.1:9000", Status: "active", LifecycleProtocol: lifecycleProtocol,
	})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", payload)), Index: 1}))
}
