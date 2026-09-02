package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type metadataReplayHandler struct {
	messages map[int][]types.Message
	starts   map[int]uint64
}

func (h *metadataReplayHandler) Publish(string, *types.Message) error       { return nil }
func (h *metadataReplayHandler) CreateTopic(string, int, bool, bool) error  { return nil }
func (h *metadataReplayHandler) ExistingPartitionCount(string) (int, error) { return 4, nil }
func (h *metadataReplayHandler) EarliestTopicOffset(_ string, partition int) (uint64, error) {
	return h.starts[partition], nil
}
func (h *metadataReplayHandler) ReadTopicPartition(_ string, partition int, offset uint64, max int) ([]types.Message, error) {
	var result []types.Message
	for _, message := range h.messages[partition] {
		if message.Offset < offset {
			continue
		}
		result = append(result, message)
		if len(result) == max {
			break
		}
	}
	return result, nil
}

func TestConsumerMetadataReplayRequiresMigrationForRetainedGap(t *testing.T) {
	handler := &metadataReplayHandler{starts: map[int]uint64{0: 42}}
	coordinator, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), handler)
	require.ErrorContains(t, err, "starts at offset 42; explicit migration selection is required")
	require.False(t, coordinator.RecoverySnapshot().Ready)
	require.Equal(t, 1, coordinator.RecoverySnapshot().OrphanRecords)
	require.Empty(t, coordinator.ListGroups())
}

func TestStandaloneRecoveryRejectsUnversionedOffsetRecord(t *testing.T) {
	handler := &metadataReplayHandler{messages: map[int][]types.Message{
		0: {{Offset: 0, Payload: `{"group":"workers","topic":"events","partition":0,"offset":7}`}},
	}}
	coordinator, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), handler)
	require.ErrorContains(t, err, "clean bootstrap required")
	require.False(t, coordinator.RecoverySnapshot().Ready)
	require.Empty(t, coordinator.ListGroups())
}

func TestDistributedRecoveryUsesRaftAsSoleAuthority(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	handler := &metadataReplayHandler{
		starts: map[int]uint64{0: 42},
		messages: map[int][]types.Message{
			0: {
				{Offset: 42, Payload: "local internal-topic state is non-authoritative"},
			},
		},
	}

	recovered, err := NewCoordinatorWithRecovery(context.Background(), cfg, handler)
	require.NoError(t, err)
	require.True(t, recovered.RecoverySnapshot().Ready)
	require.Zero(t, recovered.RecoverySnapshot().ReplayedRecords)
	require.Empty(t, recovered.ListGroups())
}

func TestConsumerMetadataReplayIsDeterministicAcrossPartitions(t *testing.T) {
	registration := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
		Group: "workers", Topic: "events", PartitionCount: 2, Epoch: 1, Timestamp: time.Unix(1, 0).UTC(),
	}
	revision1 := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
		Group: "workers", Topic: "events", Epoch: 1, Revision: 1,
		Offsets: []OffsetItem{{Partition: 0, Offset: 7}}, Timestamp: time.Unix(2, 0).UTC(),
	}
	revision2 := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
		Group: "workers", Topic: "events", Epoch: 1, Revision: 2,
		Offsets: []OffsetItem{{Partition: 0, Offset: 11}}, Timestamp: time.Unix(3, 0).UTC(),
	}
	handler := &metadataReplayHandler{messages: map[int][]types.Message{
		0: {encodedMetadataMessage(t, revision2, 0)},
		2: {encodedMetadataMessage(t, revision1, 0)},
		3: {encodedMetadataMessage(t, registration, 0)},
	}}

	coordinator, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), handler)
	require.NoError(t, err)
	offset, found := coordinator.GetOffset("workers", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(11), offset)
	require.True(t, coordinator.RecoverySnapshot().Ready)
}

func TestConsumerMetadataReplayRejectsRegressionAndDroppedKeys(t *testing.T) {
	tests := []struct {
		name     string
		first    []OffsetItem
		second   []OffsetItem
		contains string
	}{
		{name: "regression", first: []OffsetItem{{Partition: 0, Offset: 10}}, second: []OffsetItem{{Partition: 0, Offset: 9}}, contains: "offset regression during replay"},
		{name: "dropped key", first: []OffsetItem{{Partition: 0, Offset: 10}, {Partition: 1, Offset: 4}}, second: []OffsetItem{{Partition: 0, Offset: 11}}, contains: "dropped committed key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registration := ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "events", PartitionCount: 2, Epoch: 1, Timestamp: time.Unix(1, 0).UTC(),
			}
			first := ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
				Group: "workers", Topic: "events", Epoch: 1, Revision: 1, Offsets: test.first, Timestamp: time.Unix(2, 0).UTC(),
			}
			second := ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
				Group: "workers", Topic: "events", Epoch: 1, Revision: 2, Offsets: test.second, Timestamp: time.Unix(3, 0).UTC(),
			}
			handler := &metadataReplayHandler{messages: map[int][]types.Message{
				0: {encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, first, 1), encodedMetadataMessage(t, second, 2)},
			}}

			coordinator, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), handler)
			require.ErrorContains(t, err, test.contains)
			require.False(t, coordinator.RecoverySnapshot().Ready)
			require.Equal(t, 1, coordinator.RecoverySnapshot().CorruptRecords)
			require.Empty(t, coordinator.ListGroups(), "failed replay must not expose a partial broker state")
		})
	}
}

func encodedMetadataMessage(t *testing.T, record ConsumerMetadataRecord, offset uint64) types.Message {
	t.Helper()
	payload, key, err := encodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	return types.Message{Offset: offset, Key: key, Payload: string(payload)}
}
