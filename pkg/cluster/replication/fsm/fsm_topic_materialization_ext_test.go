package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type blockingTopicHandlerProvider struct {
	started sync.Once
	ready   chan struct{}
	release chan struct{}
}

func (p *blockingTopicHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	p.started.Do(func() { close(p.ready) })
	<-p.release
	return &MockStorageHandler{}, nil
}

type recoverableCleanupHandlerProvider struct {
	fail atomic.Bool
}

func (p *recoverableCleanupHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return &MockStorageHandler{}, nil
}

func (p *recoverableCleanupHandlerProvider) RemoveTopicStorage(path string) error {
	if p.fail.Load() {
		return errors.New("injected physical cleanup failure")
	}
	return os.RemoveAll(path)
}

func emptyTopicSnapshot(t *testing.T) io.ReadCloser {
	t.Helper()
	data, err := json.Marshal(BrokerFSMState{
		Version:           6,
		TopicState:        map[string]*topic.Definition{},
		PartitionMetadata: map[string]*PartitionMetadata{},
	})
	require.NoError(t, err)
	return io.NopCloser(bytes.NewReader(data))
}

func TestBrokerFSMRestoreRemovesLocalTopicMissingFromSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("stale", 1, false, false))
	f := NewBrokerFSM(manager, nil)

	require.NoError(t, f.Restore(emptyTopicSnapshot(t)))
	require.Nil(t, manager.GetTopic("stale"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}

func TestBrokerFSMRestorePreservesPendingDeleteUntilRetrySucceeds(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("stale", 1, false, false))

	manifestPath := filepath.Join(cfg.LogDir, topic.TopicMetadataFileName)
	require.NoError(t, os.Remove(manifestPath))
	require.NoError(t, os.Mkdir(manifestPath, 0o750))

	f := NewBrokerFSM(manager, nil)
	f.topicMaterialization["stale"] = TopicMaterializationIssue{
		Topic: "stale", Operation: TopicMaterializationDelete, Error: "previous failure", PendingSince: time.Now().Add(-time.Minute),
	}

	require.NoError(t, f.Restore(emptyTopicSnapshot(t)))
	require.Error(t, f.TopicMaterializationReadinessError())
	require.Equal(t, TopicMaterializationDelete, f.TopicMaterializationIssues()[0].Operation)

	require.NoError(t, os.RemoveAll(manifestPath))
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.Nil(t, manager.GetTopic("stale"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}

func TestBrokerFSMRestoreSerializesWithReconcileAndCleansStaleResult(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	provider := &blockingTopicHandlerProvider{ready: make(chan struct{}), release: make(chan struct{})}
	manager := topic.NewTopicManager(cfg, provider, nil)
	f := NewBrokerFSM(manager, nil)
	definition := &topic.Definition{Name: "stale", Partitions: 1, Policy: topic.DefaultPolicy()}
	f.topicState[definition.Name] = definition
	f.topicMaterialization[definition.Name] = TopicMaterializationIssue{
		Topic: definition.Name, Operation: TopicMaterializationCreate, Error: "pending", PendingSince: time.Now(),
	}

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- f.ReconcileTopicMaterializations() }()
	<-provider.ready

	restoreInput := emptyTopicSnapshot(t)
	restoreDone := make(chan error, 1)
	go func() { restoreDone <- f.Restore(restoreInput) }()
	select {
	case err := <-restoreDone:
		t.Fatalf("restore bypassed materialization barrier: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(provider.release)
	require.NoError(t, <-reconcileDone)
	require.NoError(t, <-restoreDone)
	require.Nil(t, manager.GetTopic("stale"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}

func TestTopicMaterializationRuntimeSnapshotCountsResultsAndAge(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	f.topicMaterialization["orders"] = TopicMaterializationIssue{
		Topic: "orders", Operation: TopicMaterializationCreate, Error: errors.New("disk").Error(), PendingSince: time.Now().Add(-time.Second),
	}
	f.topicMaterializationRuns[TopicMaterializationCreate] = TopicMaterializationAttempts{Success: 2, Failure: 3}

	snapshot := f.TopicMaterializationRuntimeSnapshot()
	require.Equal(t, 1, snapshot.PendingByOperation[TopicMaterializationCreate])
	require.Equal(t, uint64(2), snapshot.AttemptsByOperation[TopicMaterializationCreate].Success)
	require.Equal(t, uint64(3), snapshot.AttemptsByOperation[TopicMaterializationCreate].Failure)
	require.GreaterOrEqual(t, snapshot.OldestPending, time.Second)
}

func TestBrokerFSMPhysicalCleanupFailureRemainsPendingUntilRetry(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	provider := &recoverableCleanupHandlerProvider{}
	provider.fail.Store(true)
	manager := topic.NewTopicManager(cfg, provider, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	f := NewBrokerFSM(manager, nil)
	definition := topic.Definition{Name: "orders", Partitions: 1, Policy: topic.DefaultPolicy()}
	f.topicState["orders"] = copyTopicDefinition(&definition)
	f.partitionMetadata["orders-0"] = &PartitionMetadata{PartitionCount: 1}

	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 1})
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorContains(t, applyErr, "injected physical cleanup failure")
	require.Nil(t, f.topicState["orders"])
	require.Nil(t, manager.GetTopic("orders"))
	require.Error(t, f.TopicMaterializationReadinessError())
	require.Equal(t, TopicMaterializationDelete, f.TopicMaterializationIssues()[0].Operation)

	provider.fail.Store(false)
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.NoError(t, f.TopicMaterializationReadinessError())
	require.NoDirExists(t, filepath.Join(cfg.LogDir, "orders"))
}

func TestBrokerFSMRestoreCleansPersistedTopicMissingFromRegistryAndSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	staleDir := filepath.Join(cfg.LogDir, "stale")
	require.NoError(t, os.MkdirAll(staleDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(staleDir, "partition_0_segment_00000000000000000000.log"),
		nil,
		0o600,
	))

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	require.Empty(t, manager.ExportDefinitions())
	f := NewBrokerFSM(manager, nil)

	require.NoError(t, f.Restore(emptyTopicSnapshot(t)))
	require.NoDirExists(t, staleDir)
	require.NoError(t, f.TopicMaterializationReadinessError())
}
