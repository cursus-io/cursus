package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestRestoreRejectsLegacySnapshotsBeforeMutatingState(t *testing.T) {
	for _, version := range []int{7, 8} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			brokerFSM := newTestFSM()
			brokerFSM.brokers["sentinel"] = &BrokerInfo{ID: "sentinel"}

			err := brokerFSM.Restore(io.NopCloser(bytes.NewBufferString(
				fmt.Sprintf(`{"version":%d,"brokers":{"replacement":{"id":"replacement"}}}`, version),
			)))

			require.ErrorIs(t, err, ErrUnsupportedRecoveryProtocol)
			require.Contains(t, err.Error(), "clean bootstrap")
			require.NotNil(t, brokerFSM.GetBroker("sentinel"))
			require.Nil(t, brokerFSM.GetBroker("replacement"))
		})
	}
}

func TestRestoreVersionNineRejectsMissingHWMMarkerBeforeMaterialization(t *testing.T) {
	definition := snapshotTopicDefinition("orders", 1)
	state := map[string]interface{}{
		"version": SnapshotVersionCurrent,
		"topicState": map[string]interface{}{
			"orders": definition,
		},
		"partitionMetadata": map[string]interface{}{
			"orders-0": map[string]interface{}{
				"leader": "broker-1", "replicas": []string{"broker-1"}, "isr": []string{"broker-1"},
				"leader_epoch": 1, "committed_hwm": 0, "partition_count": 1,
				"lifecycle_epoch": topic.InitialLifecycleEpoch,
			},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	brokerFSM := newTestFSM()
	err = brokerFSM.Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorIs(t, err, ErrUnsupportedRecoveryProtocol)
	require.Nil(t, brokerFSM.tm.GetTopic("orders"))
	require.Nil(t, brokerFSM.GetPartitionMetadata("orders-0"))
}

func TestRestoreVersionNineRejectsScalarProducerSequence(t *testing.T) {
	brokerFSM := newTestFSM()
	err := brokerFSM.Restore(io.NopCloser(bytes.NewBufferString(
		`{"version":9,"producerState":{"orders":{"0":{"producer-1":7}}}}`,
	)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "producerState")
}

func TestPartitionMetadataRejectsInvalidAuthoritativeHWMEncoding(t *testing.T) {
	tests := []string{
		`{"committed_hwm_version":2,"committed_hwm":0}`,
		`{"committed_hwm_version":1}`,
		`{"committed_hwm_version":1,"committed_hwm":0}`,
	}
	for _, encoded := range tests {
		var metadata PartitionMetadata
		require.ErrorIs(t, json.Unmarshal([]byte(encoded), &metadata), ErrUnsupportedRecoveryProtocol)
	}
}

func TestRestoreVersionNineRejectsUnknownFields(t *testing.T) {
	brokerFSM := newTestFSM()
	err := brokerFSM.Restore(io.NopCloser(bytes.NewBufferString(
		`{"version":9,"unsupported_recovery_field":true}`,
	)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestTopicCommandRequiresAuthoritativeHWMMarker(t *testing.T) {
	brokerFSM := newTestFSM()
	registerActiveBroker(t, brokerFSM, "broker-1")

	result := brokerFSM.Apply(&raft.Log{
		Index: 2,
		Data:  []byte(`TOPIC:{"name":"orders","partitions":1,"replication_factor":1}`),
	})
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, ErrUnsupportedRecoveryProtocol)
	require.Nil(t, brokerFSM.GetPartitionMetadata("orders-0"))

	command, err := json.Marshal(TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	require.Contains(t, string(command), `"committed_hwm_version":1`)
}
