package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type recoverableTopicHandlerProvider struct {
	fail atomic.Bool
}

func (p *recoverableTopicHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	if p.fail.Load() {
		return nil, errors.New("open partition failed")
	}
	return &MockStorageHandler{}, nil
}

func TestBrokerFSMTopicCreateFailureCommitsDesiredStateAndReconciles(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	provider := &recoverableTopicHandlerProvider{}
	provider.fail.Store(true)
	manager := topic.NewTopicManager(cfg, provider, nil)
	f := NewBrokerFSM(manager, nil)
	registerActiveBroker(t, f, "broker-1")

	command, err := json.Marshal(TopicCommand{
		Name:              "orders",
		Partitions:        1,
		ReplicationFactor: 1,
		Policy:            topic.DefaultPolicy(),
	})
	require.NoError(t, err)

	result := f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(command)), Index: 2})
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorContains(t, applyErr, "open partition failed")
	require.NotNil(t, f.GetPartitionMetadata("orders-0"))
	require.NotNil(t, f.topicState["orders"])
	require.Nil(t, manager.GetTopic("orders"))
	require.Error(t, f.TopicMaterializationReadinessError())
	require.Len(t, f.TopicMaterializationIssues(), 1)
	require.Equal(t, TopicMaterializationCreate, f.TopicMaterializationIssues()[0].Operation)

	provider.fail.Store(false)
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.NotNil(t, manager.GetTopic("orders"))
	require.NoError(t, f.TopicMaterializationReadinessError())
	require.Empty(t, f.TopicMaterializationIssues())
}

func TestBrokerFSMTopicConfigFailureCommitsDesiredStateAndReconciles(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(manager.Stop)
	f := NewBrokerFSM(manager, nil)
	registerActiveBroker(t, f, "broker-1")

	create, err := json.Marshal(TopicCommand{
		Name:              "orders",
		Partitions:        1,
		ReplicationFactor: 1,
		Policy:            topic.DefaultPolicy(),
	})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 2}))

	metadataPath := filepath.Join(cfg.LogDir, topic.TopicMetadataFileName)
	require.NoError(t, os.Remove(metadataPath))
	require.NoError(t, os.Mkdir(metadataPath, 0o750))
	one := 1
	configCommand, err := json.Marshal(TopicConfigCommand{Name: "orders", MinInSyncReplicas: &one})
	require.NoError(t, err)

	result := f.Apply(&raft.Log{Data: []byte("TOPIC_CONFIG:" + string(configCommand)), Index: 3})
	require.Error(t, result.(error))
	authoritative, found := f.GetTopicDefinition("orders")
	require.True(t, found)
	require.Equal(t, uint64(2), authoritative.Revision)
	require.Equal(t, 1, *authoritative.Policy.MinInSyncReplicas)
	require.Equal(t, uint64(1), manager.GetTopic("orders").Revision)
	require.Nil(t, manager.GetTopic("orders").Policy.MinInSyncReplicas)
	require.Equal(t, TopicMaterializationCreate, f.TopicMaterializationIssues()[0].Operation)

	require.NoError(t, os.RemoveAll(metadataPath))
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.Equal(t, uint64(2), manager.GetTopic("orders").Revision)
	require.Equal(t, 1, *manager.GetTopic("orders").Policy.MinInSyncReplicas)
	require.Empty(t, f.TopicMaterializationIssues())
}

func TestBrokerFSMDeleteFailureCommitsDesiredStateAndReconciles(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(cfg.LogDir, topic.TopicMetadataFileName), 0o750))

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	definition := topic.Definition{Name: "orders", Partitions: 1, Policy: topic.DefaultPolicy()}
	require.NoError(t, manager.RestoreDefinitions([]topic.Definition{definition}))
	t.Cleanup(func() {
		if current := manager.GetTopic("orders"); current != nil {
			for _, partition := range current.Partitions {
				partition.Close()
			}
		}
	})

	f := NewBrokerFSM(manager, nil)
	f.topicState["orders"] = copyTopicDefinition(&definition)
	f.partitionMetadata["orders-0"] = &PartitionMetadata{PartitionCount: 1}
	payload, err := json.Marshal(map[string]string{"topic": "orders"})
	require.NoError(t, err)

	result := f.Apply(&raft.Log{Data: []byte("TOPIC_DELETE:" + string(payload)), Index: 1})
	require.Equal(t, topic.DeleteResult{Deleted: true, CleanupPending: true}, result)
	require.Nil(t, f.GetPartitionMetadata("orders-0"))
	require.Nil(t, f.topicState["orders"])
	require.NotNil(t, manager.GetTopic("orders"))
	require.Error(t, f.TopicMaterializationReadinessError())

	require.NoError(t, os.RemoveAll(filepath.Join(cfg.LogDir, topic.TopicMetadataFileName)))
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.Nil(t, manager.GetTopic("orders"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}

func TestBrokerFSMDeleteMissingTopicReturnsNotFound(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"missing"}`), Index: 1})
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, topic.ErrTopicNotFound)
}

func TestBrokerFSMRestoreRejectsIncompleteLegacyPartitionMetadata(t *testing.T) {
	state := BrokerFSMState{
		Version: SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{
			"orders": snapshotTopicDefinition("orders", 2),
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": authoritativePartitionMetadata(2),
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "missing partition metadata 1")
}

func TestBrokerFSMRestoreRejectsTopicMetadataConflicts(t *testing.T) {
	tests := []struct {
		name     string
		metadata *PartitionMetadata
		want     string
	}{
		{
			name:     "partition count",
			metadata: authoritativePartitionMetadata(2),
			want:     "declares partition count 2",
		},
		{
			name:     "idempotent mode",
			metadata: authoritativePartitionMetadata(1),
			want:     "idempotent mode conflicts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metadata.Idempotent = tt.name == "partition count"
			state := BrokerFSMState{
				Version: SnapshotVersionCurrent,
				TopicState: map[string]*topic.Definition{
					"orders": func() *topic.Definition {
						definition := snapshotTopicDefinition("orders", 1)
						definition.Idempotent = true
						return definition
					}(),
				},
				PartitionMetadata: map[string]*PartitionMetadata{"orders-0": tt.metadata},
			}
			data, err := json.Marshal(state)
			require.NoError(t, err)

			err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestBrokerFSMRestoreDefersLocalTopicMaterializationFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	provider := &recoverableTopicHandlerProvider{}
	provider.fail.Store(true)
	manager := topic.NewTopicManager(cfg, provider, nil)
	f := NewBrokerFSM(manager, nil)
	state := BrokerFSMState{
		Version: SnapshotVersionCurrent,
		TopicState: map[string]*topic.Definition{
			"orders": snapshotTopicDefinition("orders", 1),
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": authoritativePartitionMetadata(1),
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(data))))
	require.NotNil(t, f.GetPartitionMetadata("orders-0"))
	require.Nil(t, manager.GetTopic("orders"))
	require.Error(t, f.TopicMaterializationReadinessError())

	provider.fail.Store(false)
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.NotNil(t, manager.GetTopic("orders"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}

func TestBrokerFSMDeleteSupersedesPendingCreateWithoutLocalTopic(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	provider := &recoverableTopicHandlerProvider{}
	provider.fail.Store(true)
	manager := topic.NewTopicManager(cfg, provider, nil)
	f := NewBrokerFSM(manager, nil)
	registerActiveBroker(t, f, "broker-1")

	command, err := json.Marshal(TopicCommand{
		Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy(),
	})
	require.NoError(t, err)
	require.Error(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(command)), Index: 2}).(error))
	require.Error(t, f.TopicMaterializationReadinessError())

	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 3})
	require.Nil(t, result)
	require.Nil(t, f.GetPartitionMetadata("orders-0"))
	require.Nil(t, manager.GetTopic("orders"))
	require.NoError(t, f.TopicMaterializationReadinessError())
}
