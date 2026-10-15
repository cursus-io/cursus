package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// registrationActivityReader measures scan calls, returned records, and payload
// bytes so registration recovery cannot silently add another log scan.
type registrationActivityReader struct {
	messages                     []types.Message
	calls, records, payloadBytes int
}

// Publish satisfies coordinator setup without changing the replay fixture.
func (*registrationActivityReader) Publish(string, *types.Message) error { return nil }

// CreateTopic keeps the fixture's internal topic available without storage setup.
func (*registrationActivityReader) CreateTopic(string, int, bool, bool) error { return nil }

// ExistingPartitionCount exposes four partitions, including three empty scans.
func (*registrationActivityReader) ExistingPartitionCount(string) (int, error) { return 4, nil }

// ReadTopicPartition returns the requested fixture batch and counts its actual
// payload bytes, including repeated reads if recovery scans a record again.
func (r *registrationActivityReader) ReadTopicPartition(_ string, partition int, offset uint64, max int) ([]types.Message, error) {
	r.calls++
	var result []types.Message
	if partition == 0 {
		for _, message := range r.messages {
			if message.Offset >= offset && len(result) < max {
				result = append(result, message)
				r.records++
				r.payloadBytes += len(message.Payload)
			}
		}
	}
	return result, nil
}

// recoverRegistrationActivity replays one distributed registration and verifies
// readiness and scan cost without assuming its timestamp is known.
func recoverRegistrationActivity(t *testing.T, registration coordinator.ConsumerMetadataRecord) (*coordinator.Coordinator, *registrationActivityReader) {
	t.Helper()
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(registration)
	require.NoError(t, err)
	reader := &registrationActivityReader{messages: []types.Message{{Key: key, Payload: string(payload)}}}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, reader)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.True(t, c.RecoverySnapshot().Ready)
	require.Equal(t, 4, reader.calls, "one short batch per partition; no extra scan")
	require.Equal(t, 1, reader.records)
	require.Equal(t, len(payload), reader.payloadBytes)
	return c, reader
}

// TestDistributedRegistrationActivitySnapshotRoundTrip preserves exact registration
// activity and group state through the real FSM snapshot, persist, and restore path.
func TestDistributedRegistrationActivitySnapshotRoundTrip(t *testing.T) {
	registeredAt := time.Unix(100, 123).UTC()
	for _, test := range []struct {
		name   string
		record coordinator.ConsumerMetadataRecord
	}{
		{"single", coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Topic: "orders", PartitionCount: 2}},
		{"subscription", coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersionSubscriptions, Topics: []string{"orders", "payments"}, TopicPartitions: []coordinator.TopicPartition{{Topic: "orders", Partition: 0}, {Topic: "payments", Partition: 0}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := test.record
			record.Type, record.Group, record.Epoch, record.Timestamp = coordinator.ConsumerMetadataRecordRegistration, "workers", 7, registeredAt
			c, reader := recoverRegistrationActivity(t, record)
			before := c.ExportState()["workers"]
			require.Equal(t, registeredAt, before.LastActivity)
			require.Zero(t, before.Generation)
			require.Empty(t, before.Members)
			require.Empty(t, before.Offsets)
			require.Equal(t, uint64(7), before.RegistrationEpoch)
			if test.name == "single" {
				require.Equal(t, []int{0, 1}, before.Partitions)
			} else {
				require.Equal(t, record.TopicPartitions, before.TopicPartitions)
				require.Equal(t, record.Topics, before.Topics)
			}
			snapshot, err := NewBrokerFSM(nil, c).Snapshot()
			require.NoError(t, err)
			defer snapshot.Release()
			var buf bytes.Buffer
			require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &buf}))
			restored, err := coordinator.NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &registrationActivityReader{})
			require.NoError(t, err)
			t.Cleanup(restored.Stop)
			require.NoError(t, NewBrokerFSM(nil, restored).Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
			require.Equal(t, before, restored.ExportState()["workers"])
			require.Equal(t, 4, reader.calls)
			require.Equal(t, 1, reader.records)
		})
	}
}

// TestDistributedRegistrationActivityUnknownRefusesSnapshot permits historical
// unknown timestamps during replay but prevents publishing an unrestorable snapshot.
func TestDistributedRegistrationActivityUnknownRefusesSnapshot(t *testing.T) {
	c, _ := recoverRegistrationActivity(t, coordinator.ConsumerMetadataRecord{
		Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration,
		Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1,
	})
	require.True(t, c.ExportState()["workers"].LastActivity.IsZero())
	snapshot, err := NewBrokerFSM(nil, c).Snapshot()
	require.ErrorContains(t, err, "missing last activity")
	require.Nil(t, snapshot)
}

// TestRegistrationActivityRestoreRejectsExistingInvalidSnapshotAtomically verifies
// that old zero-activity snapshots cannot replace valid installed group state.
func TestRegistrationActivityRestoreRejectsExistingInvalidSnapshotAtomically(t *testing.T) {
	c, _ := recoverRegistrationActivity(t, coordinator.ConsumerMetadataRecord{
		Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration,
		Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1, Timestamp: time.Unix(100, 0).UTC(),
	})
	before := c.ExportState()
	invalid := c.ExportState()
	invalid["workers"].LastActivity = time.Time{}
	// Model bytes persisted by the old snapshot writer without bypassing Restore.
	nextEpoch := uint64(0)
	payload, err := json.Marshal(BrokerFSMState{Version: SnapshotVersionCurrent, GroupState: invalid, NextProducerEpoch: &nextEpoch})
	require.NoError(t, err)
	err = NewBrokerFSM(nil, c).Restore(io.NopCloser(bytes.NewReader(payload)))
	require.ErrorContains(t, err, "missing last activity")
	require.Equal(t, before, c.ExportState())
}
