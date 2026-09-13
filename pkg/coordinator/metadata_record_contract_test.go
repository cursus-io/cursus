package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplyOffsetUpdateFromFSMInitializesAndFencesReplayState(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(c.Stop)

	require.ErrorContains(t, c.ApplyOffsetUpdateFromFSM("", "orders", nil), "invalid group or topic")
	require.ErrorContains(t, c.ApplyOffsetUpdateFromFSM("workers", "", nil), "invalid group or topic")
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("workers", "orders", []OffsetItem{{Partition: 0, Offset: 4}}))
	offset, found := c.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(4), offset)
	require.NotNil(t, c.GetGroup("workers").Members)

	c.mu.Lock()
	c.groups["blank"] = &GroupMetadata{}
	c.mu.Unlock()
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("blank", "payments", []OffsetItem{{Partition: 1, Offset: 7}}))
	blank := c.GetGroup("blank")
	require.Equal(t, "payments", blank.TopicName)
	require.NotNil(t, blank.Members)
	require.NotNil(t, blank.Offsets)
	require.ErrorContains(t, c.ApplyOffsetUpdateFromFSM("blank", "payments", []OffsetItem{{Partition: 1, Offset: 6}}), "offset regression")
}

func TestMaterializeCommittedTransactionOffsetsIsAtomic(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(c.Stop)
	c.groups["workers"] = &GroupMetadata{
		Topics: []string{"orders", "payments"},
		TopicPartitions: []TopicPartition{
			{Topic: "orders", Partition: 0},
			{Topic: "payments", Partition: 0},
		},
		Members:           make(map[string]*MemberMetadata),
		Offsets:           make(map[string]map[int]uint64),
		RegistrationEpoch: 3,
		OffsetRevisions:   make(map[string]uint64),
	}

	require.NoError(t, c.MaterializeCommittedTransactionOffsets("workers", 3, nil))
	require.ErrorContains(t, c.MaterializeCommittedTransactionOffsets("missing", 3, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: 1}}}), "not found")
	require.ErrorContains(t, c.MaterializeCommittedTransactionOffsets("workers", 2, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: 1}}}), "group_epoch_mismatch")

	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return fmt.Errorf("append unavailable") })
	require.ErrorContains(t, c.MaterializeCommittedTransactionOffsets("workers", 3, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: 4}}}), "append unavailable")
	_, found := c.GetOffset("workers", "orders", 0)
	require.False(t, found, "a failed durable append must not expose an offset")

	var records []ConsumerMetadataRecord
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, c.MaterializeCommittedTransactionOffsets("workers", 3, map[string][]OffsetItem{
		"payments": {{Partition: 0, Offset: 8}},
		"orders":   {{Partition: 0, Offset: 5}},
	}))
	require.Len(t, records, 2)
	require.Equal(t, "orders", records[0].Topic, "durable writes must use deterministic topic order")
	require.Equal(t, "payments", records[1].Topic)
	require.Equal(t, uint64(5), mustOffset(t, c, "workers", "orders", 0))
	require.Equal(t, uint64(8), mustOffset(t, c, "workers", "payments", 0))
	require.Equal(t, uint64(1), c.GetGroup("workers").OffsetRevisions["orders"])
}

func TestConsumerMetadataCandidateSelectionRejectsConflicts(t *testing.T) {
	registration := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
		Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 2,
	}
	status := ConsumerMetadataRecoveryStatus{}
	candidates := newConsumerMetadataCandidates()
	require.NoError(t, candidates.selectRecord(registration, &status))

	older := registration
	older.Epoch = 1
	require.NoError(t, candidates.selectRecord(older, &status))
	require.Equal(t, 1, status.OrphanRecords)

	newer := registration
	newer.Epoch = 3
	require.NoError(t, candidates.selectRecord(newer, &status))
	require.Equal(t, 2, status.OrphanRecords)
	require.NoError(t, candidates.selectRecord(newer, &status))
	require.Equal(t, 3, status.OrphanRecords)

	conflict := newer
	conflict.Topic = "payments"
	require.ErrorContains(t, candidates.selectRecord(conflict, &status), "conflicting lifecycle records")
	require.Equal(t, 1, status.CorruptRecords)

	offset := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
		Group: "workers", Topic: "orders", Epoch: 3, Revision: 1,
		Offsets: []OffsetItem{{Partition: 0, Offset: 4}},
	}
	require.NoError(t, candidates.selectRecord(offset, &status))
	require.NoError(t, candidates.selectRecord(offset, &status))
	conflictingOffset := offset
	conflictingOffset.Offsets = []OffsetItem{{Partition: 0, Offset: 5}}
	require.ErrorContains(t, candidates.selectRecord(conflictingOffset, &status), "conflicting offset snapshots")

	transactional := offset
	transactional.Version = ConsumerMetadataRecordVersionTransactions
	transactional.Type = ConsumerMetadataRecordTransactionalOffsetSnapshot
	before := status.OffsetRecords
	require.NoError(t, candidates.selectRecord(transactional, &status))
	require.Equal(t, before+1, status.OffsetRecords)
}

func TestLifecycleSnapshotSelectionIsMonotonic(t *testing.T) {
	base := validLifecycleRecord()
	candidates := make(map[string]lifecycleSnapshotCandidate)
	status := ConsumerMetadataRecoveryStatus{}
	require.NoError(t, selectLifecycleSnapshot(candidates, base, &status))

	newer := base
	newer.Revision = 2
	newer.Lifecycle = cloneLifecycle(base.Lifecycle)
	newer.Lifecycle.Generation = 2
	require.NoError(t, selectLifecycleSnapshot(candidates, newer, &status))
	require.Equal(t, 1, status.OrphanRecords)
	require.NoError(t, selectLifecycleSnapshot(candidates, base, &status))
	require.NoError(t, selectLifecycleSnapshot(candidates, newer, nil))

	conflict := newer
	conflict.Lifecycle = cloneLifecycle(newer.Lifecycle)
	conflict.Lifecycle.Members = []GroupLifecycleMember{{ID: "other"}}
	require.ErrorContains(t, selectLifecycleSnapshot(candidates, conflict, &status), "conflicting lifecycle snapshots")
	require.Equal(t, 1, status.CorruptRecords)
}

func TestConsumerMetadataValidationRejectsMalformedLifecycleRecords(t *testing.T) {
	valid := validLifecycleRecord()
	require.NoError(t, validateConsumerMetadataRecord(valid))

	tests := []struct {
		name     string
		mutate   func(*ConsumerMetadataRecord)
		contains string
	}{
		{name: "version four wrong type", mutate: func(r *ConsumerMetadataRecord) { r.Type = ConsumerMetadataRecordTombstone }, contains: "version 4 requires"},
		{name: "missing lifecycle", mutate: func(r *ConsumerMetadataRecord) { r.Lifecycle = nil }, contains: "missing version or state"},
		{name: "non lifecycle fields", mutate: func(r *ConsumerMetadataRecord) { r.Topic = "orders" }, contains: "non-lifecycle fields"},
		{name: "invalid generation", mutate: func(r *ConsumerMetadataRecord) {
			r.Lifecycle = cloneLifecycle(r.Lifecycle)
			r.Lifecycle.Generation = -1
		}, contains: "invalid generation"},
		{name: "revision mismatch", mutate: func(r *ConsumerMetadataRecord) { r.Revision++ }, contains: "revision does not match generation"},
		{name: "empty member", mutate: func(r *ConsumerMetadataRecord) {
			r.Lifecycle = cloneLifecycle(r.Lifecycle)
			r.Lifecycle.Members = []GroupLifecycleMember{{}}
		}, contains: "empty member"},
		{name: "duplicate member", mutate: func(r *ConsumerMetadataRecord) {
			r.Lifecycle = cloneLifecycle(r.Lifecycle)
			r.Lifecycle.Members = []GroupLifecycleMember{{ID: "member"}, {ID: "member"}}
		}, contains: "duplicate member"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			record.Lifecycle = cloneLifecycle(valid.Lifecycle)
			test.mutate(&record)
			require.ErrorContains(t, validateConsumerMetadataRecord(record), test.contains)
		})
	}
}

func TestConsumerMetadataPublicCodecFailsClosed(t *testing.T) {
	record := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
		Group: "workers", Topic: "orders", Epoch: 2, Revision: 1,
		Offsets:   []OffsetItem{{Partition: 1, Offset: 9}, {Partition: 0, Offset: 4}},
		Timestamp: time.Unix(10, 0),
	}
	payload, key, err := EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	require.NotEmpty(t, key)
	require.Equal(t, "cursus.consumer.partition.v1.workers", ConsumerMetadataGroupPartitionKey("workers"))
	decoded, versioned, err := DecodeConsumerMetadataRecord(string(payload))
	require.NoError(t, err)
	require.True(t, versioned)
	require.Equal(t, []OffsetItem{{Partition: 0, Offset: 4}, {Partition: 1, Offset: 9}}, decoded.Offsets)

	_, _, err = EncodeConsumerMetadataRecord(ConsumerMetadataRecord{})
	require.Error(t, err)
	_, versioned, err = DecodeConsumerMetadataRecord(`{"version":"bad"}`)
	require.True(t, versioned)
	require.ErrorContains(t, err, "decode consumer metadata version")
	_, versioned, err = DecodeConsumerMetadataRecord(`{"version":1,"type":"offset_snapshot","group":"g","epoch":1,"unknown":true}`)
	require.True(t, versioned)
	require.ErrorContains(t, err, "unknown field")
	_, versioned, err = DecodeConsumerMetadataRecord(`{"group":"workers","topic":"orders"}`)
	require.False(t, versioned)
	require.NoError(t, err)

	bulk, err := json.Marshal(BulkOffsetMsg{Group: "workers", Topic: "orders", Offsets: []OffsetItem{{Partition: 0, Offset: 3}}})
	require.NoError(t, err)
	group, topic, offsets, err := DecodeLegacyOffsetPayload(string(bulk))
	require.NoError(t, err)
	require.Equal(t, "workers", group)
	require.Equal(t, "orders", topic)
	require.Equal(t, []OffsetItem{{Partition: 0, Offset: 3}}, offsets)
	_, _, _, err = DecodeLegacyOffsetPayload(`{"group":"workers"}`)
	require.ErrorContains(t, err, "missing group or topic")
	_, _, _, err = DecodeLegacyOffsetPayload(`{`)
	require.Error(t, err)
}

func validLifecycleRecord() ConsumerMetadataRecord {
	return ConsumerMetadataRecord{
		Version:  ConsumerMetadataRecordVersionLifecycle,
		Type:     ConsumerMetadataRecordLifecycleSnapshot,
		Group:    "workers",
		Epoch:    2,
		Revision: 1,
		Lifecycle: &GroupLifecycleSnapshot{
			TopicName: "orders", Generation: 1,
			Members:    []GroupLifecycleMember{{ID: "member"}},
			Partitions: []int{0},
		},
	}
}

func cloneLifecycle(source *GroupLifecycleSnapshot) *GroupLifecycleSnapshot {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Members = append([]GroupLifecycleMember(nil), source.Members...)
	clone.Partitions = append([]int(nil), source.Partitions...)
	return &clone
}

func mustOffset(t *testing.T, c *Coordinator, group, topic string, partition int) uint64 {
	t.Helper()
	offset, found := c.GetOffset(group, topic, partition)
	require.True(t, found)
	return offset
}
