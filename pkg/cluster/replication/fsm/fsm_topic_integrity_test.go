package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type failingTopicHandlerProvider struct{}

func (failingTopicHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return nil, errors.New("open partition failed")
}

func TestBrokerFSMTopicCreateFailureLeavesMetadataUnchanged(t *testing.T) {
	manager := topic.NewTopicManager(config.DefaultConfig(), failingTopicHandlerProvider{}, nil)
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
	require.Nil(t, f.GetPartitionMetadata("orders-0"))
	require.Nil(t, f.topicState["orders"])
	require.Nil(t, manager.GetTopic("orders"))
}

func TestBrokerFSMDeleteFailurePreservesMetadataAndRegistry(t *testing.T) {
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
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorContains(t, applyErr, "delete local topic")
	require.NotNil(t, f.GetPartitionMetadata("orders-0"))
	require.NotNil(t, f.topicState["orders"])
	require.NotNil(t, manager.GetTopic("orders"))
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
		Version: 5,
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": {PartitionCount: 2},
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
			metadata: &PartitionMetadata{PartitionCount: 2, Idempotent: true},
			want:     "declares partition count 2",
		},
		{
			name:     "idempotent mode",
			metadata: &PartitionMetadata{PartitionCount: 1, Idempotent: false},
			want:     "idempotent mode conflicts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := BrokerFSMState{
				Version: 6,
				TopicState: map[string]*topic.Definition{
					"orders": {
						Name:       "orders",
						Partitions: 1,
						Idempotent: true,
						Policy:     topic.DefaultPolicy(),
					},
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
