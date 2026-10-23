package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func cooperativeGroupFixture(t *testing.T) *GroupStateSnapshot {
	t.Helper()
	s := cooperativeJoin(t, cooperativeJoin(t, cooperativeFixture(t, 6), "a"), "b")
	snapshot := &GroupStateSnapshot{
		TopicName: "orders", Partitions: makePartitions(6), RegistrationEpoch: 19,
		Members:      map[string][]int{"a": {0, 1, 2, 3, 4, 5}, "b": {}},
		LastActivity: time.Unix(100, 0).UTC(), LastRebalance: time.Unix(100, 0).UTC(),
		Offsets: make(map[string]map[int]uint64), Cooperative: s,
	}
	require.NoError(t, ValidateImportState(map[string]*GroupStateSnapshot{"workers": snapshot}))
	return snapshot
}

func cooperativeRecoveryCoordinator(t *testing.T, snapshot *GroupStateSnapshot) *Coordinator {
	t.Helper()
	c, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportState(map[string]*GroupStateSnapshot{"workers": snapshot}))
	return c
}

func cooperativeLifecycleRecord(t *testing.T) ConsumerMetadataRecord {
	t.Helper()
	c := cooperativeRecoveryCoordinator(t, cooperativeGroupFixture(t))
	var record ConsumerMetadataRecord
	c.SetOffsetRecordWriter(func(r ConsumerMetadataRecord) error { record = r; return nil })
	require.NoError(t, c.writeGroupLifecycleSnapshot("workers", 19, c.groups["workers"]))
	require.Equal(t, ConsumerMetadataRecordVersionCooperative, record.Version)
	return record
}

func TestCooperativeMetadataAndSnapshotRecovery(t *testing.T) {
	record := cooperativeLifecycleRecord(t)
	registration := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 6, Epoch: 19, Timestamp: time.Unix(90, 0).UTC()}
	messages := []types.Message{encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, record, 1)}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	before := c.ExportState()
	require.Equal(t, record.Lifecycle.Cooperative, before["workers"].Cooperative)
	require.Len(t, cooperativeAcks(before["workers"].Cooperative, "a"), 3)
	require.NoError(t, c.ReloadDistributedConsumerMetadata())
	require.Equal(t, before["workers"].Cooperative, c.ExportState()["workers"].Cooperative)
	data, err := json.Marshal(before)
	require.NoError(t, err)
	var restored map[string]*GroupStateSnapshot
	require.NoError(t, json.Unmarshal(data, &restored))
	require.NoError(t, c.ImportState(restored))
	require.Equal(t, before, c.ExportState())
	restored["workers"].Cooperative.Current[0].Epoch++
	require.Equal(t, before, c.ExportState(), "import must own its slices")
	copy := c.ExportState()
	copy["workers"].Cooperative.Members[0].Session++
	require.Equal(t, before, c.ExportState(), "export must own its slices")
	t.Logf("metadata records=2 payload_bytes=%d snapshot_bytes=%d grants=%d pending_revocations=%d", len(messages[0].Payload)+len(messages[1].Payload), len(data), len(before["workers"].Cooperative.Current), len(cooperativeAcks(before["workers"].Cooperative, "a")))
}

func TestCooperativeRecoveryRejectsConflictingRevisionAndDowngrade(t *testing.T) {
	record := cooperativeLifecycleRecord(t)
	conflicting := canonicalConsumerMetadataRecord(record)
	conflicting.Lifecycle.Cooperative.Target[0].Member = "b"
	conflicting.Lifecycle.Cooperative.Target[3].Member = "a"
	conflicting.Lifecycle.Cooperative.Current[0].RevokeRevision = conflicting.Revision
	require.NoError(t, validateConsumerMetadataRecord(conflicting))
	for _, ordered := range []bool{false, true} {
		candidates := make(map[string]lifecycleSnapshotCandidate)
		status := &ConsumerMetadataRecoveryStatus{}
		require.NoError(t, selectLifecycleSnapshotAt(candidates, record, status, consumerMetadataRecordPosition{ordered: ordered, partition: 0, offset: 1}, ordered))
		require.ErrorContains(t, selectLifecycleSnapshotAt(candidates, conflicting, status, consumerMetadataRecordPosition{ordered: ordered, partition: 0, offset: 2}, ordered), "conflicting cooperative lifecycle revision")
	}
	for _, revision := range []uint64{record.Revision, record.Revision + 1} {
		eager := canonicalConsumerMetadataRecord(record)
		eager.Version, eager.Revision, eager.Lifecycle.Generation, eager.Lifecycle.Cooperative = 4, revision, int(revision), nil
		for _, pair := range [][2]ConsumerMetadataRecord{{record, eager}, {eager, record}} {
			candidates := make(map[string]lifecycleSnapshotCandidate)
			require.NoError(t, selectLifecycleSnapshot(candidates, pair[0], nil))
			require.ErrorContains(t, selectLifecycleSnapshot(candidates, pair[1], nil), "overlaps cooperative ownership")
		}
	}
	eager := canonicalConsumerMetadataRecord(record)
	eager.Version, eager.Revision, eager.Lifecycle.Generation, eager.Lifecycle.Cooperative = 4, 0, 0, nil
	candidates := make(map[string]lifecycleSnapshotCandidate)
	require.NoError(t, selectLifecycleSnapshotAt(candidates, record, nil, consumerMetadataRecordPosition{ordered: true, offset: 1}, true))
	require.ErrorContains(t, selectLifecycleSnapshotAt(candidates, eager, nil, consumerMetadataRecordPosition{ordered: true, offset: 2}, true), "eager lifecycle follows cooperative")
	// The same eager record is a valid historical predecessor when scanned first.
	candidates = make(map[string]lifecycleSnapshotCandidate)
	require.NoError(t, selectLifecycleSnapshotAt(candidates, eager, nil, consumerMetadataRecordPosition{ordered: true, offset: 1}, true))
	require.NoError(t, selectLifecycleSnapshotAt(candidates, record, nil, consumerMetadataRecordPosition{ordered: true, offset: 2}, true))
}

func TestCooperativeRecoveryRejectsInvalidProjectionAndFormat(t *testing.T) {
	record := cooperativeLifecycleRecord(t)
	for _, version := range []int{1, 4, 5} {
		bad := canonicalConsumerMetadataRecord(record)
		bad.Version = version
		require.Error(t, validateConsumerMetadataRecord(bad))
	}
	bad := canonicalConsumerMetadataRecord(record)
	bad.Lifecycle.Cooperative = nil
	require.Error(t, validateConsumerMetadataRecord(bad))
	bad = canonicalConsumerMetadataRecord(record)
	bad.Lifecycle.Cooperative.RegistrationEpoch++
	require.Error(t, validateConsumerMetadataRecord(bad))
	bad = canonicalConsumerMetadataRecord(record)
	bad.Lifecycle.Members[0].Assignments = nil
	require.Error(t, validateConsumerMetadataRecord(bad))
	bad = canonicalConsumerMetadataRecord(record)
	bad.Lifecycle.Generation = int(bad.Revision)
	require.Error(t, validateConsumerMetadataRecord(bad))
	bad = canonicalConsumerMetadataRecord(record)
	bad.Lifecycle.LastActivity = time.Time{}
	require.Error(t, validateConsumerMetadataRecord(bad))
	snapshot := cooperativeGroupFixture(t)
	snapshot.Deleted = true
	require.Error(t, ValidateImportState(map[string]*GroupStateSnapshot{"workers": snapshot}))
	snapshot.Deleted = false
	require.ErrorContains(t, validateCompatibilityState(map[string]*GroupStateSnapshot{"workers": snapshot}), "cannot contain cooperative")
}

func TestCooperativeFailedLifecycleWriteKeepsInstalledOwnership(t *testing.T) {
	c := cooperativeRecoveryCoordinator(t, cooperativeGroupFixture(t))
	installed := c.groups["workers"]
	before := c.ExportState()
	candidate := cloneGroupLifecycle(installed)
	var err error
	candidate.Cooperative, err = candidate.Cooperative.Acknowledge("a", candidate.Cooperative.session("a"), cooperativeAcks(candidate.Cooperative, "a"), nil)
	require.NoError(t, err)
	for _, member := range candidate.Members {
		member.Assignments = nil
	}
	for _, grant := range candidate.Cooperative.Current {
		candidate.Members[grant.Member].Assignments = append(candidate.Members[grant.Member].Assignments, grant.Partition.Partition)
	}
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		require.Equal(t, ConsumerMetadataRecordVersionCooperative, record.Version)
		require.Equal(t, before, c.ExportState())
		return errors.New("injected durable append failure")
	})
	c.lifecyclePending["workers"] = true
	require.ErrorContains(t, c.persistAndApplyLifecycle("workers", installed, candidate, publishLifecycle), "injected durable append failure")
	require.Equal(t, before, c.ExportState())
	require.False(t, c.lifecyclePending["workers"])
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	c.lifecyclePending["workers"] = true
	require.NoError(t, c.persistAndApplyLifecycle("workers", installed, candidate, publishLifecycle))
	require.Equal(t, candidate.Cooperative, c.ExportState()["workers"].Cooperative)
	candidate.Cooperative.Current[0].Epoch++
	require.NotEqual(t, candidate.Cooperative, installed.Cooperative)
}

func TestCooperativeRecoveredGroupRejectsEagerOperations(t *testing.T) {
	c := cooperativeRecoveryCoordinator(t, cooperativeGroupFixture(t))
	before := c.ExportState()
	require.ErrorIs(t, c.RegisterGroup("orders", "workers", 6), errCooperativeProtocolRequired)
	_, err := c.AddConsumer("workers", "c")
	require.ErrorIs(t, err, errCooperativeProtocolRequired)
	require.ErrorIs(t, c.RemoveConsumer("workers", "a"), errCooperativeProtocolRequired)
	require.ErrorContains(t, c.RemoveConsumerForGeneration("workers", "a", 0), "cooperative_protocol_required")
	require.ErrorIs(t, c.ExpireConsumers("workers", 0, []string{"a"}), errCooperativeProtocolRequired)
	require.Contains(t, c.ValidateMemberGeneration("workers", "a", -1), "cooperative_protocol_required")
	require.False(t, c.ValidateOwnershipAtomic("workers", "a", 0, 0))
	require.ErrorContains(t, c.RecordHeartbeat("workers", "a"), "cooperative_protocol_required")
	require.ErrorIs(t, c.applyOffsetUpdateFromFSM("workers", "orders", []OffsetItem{{Partition: 0, Offset: 1}}), errCooperativeProtocolRequired)
	require.ErrorIs(t, c.CommitOffset("workers", "orders", 0, 1), errCooperativeProtocolRequired)
	require.ErrorIs(t, c.CommitOffsetsBulk("workers", "orders", []OffsetItem{{Partition: 0, Offset: 1}}), errCooperativeProtocolRequired)
	c.Rebalance("workers")
	require.Equal(t, before, c.ExportState())
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	require.NoError(t, c.MaterializeCommittedTransactionOffsets("workers", 19, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: 1}}}))
	current := c.ExportState()["workers"]
	require.Equal(t, before["workers"].Cooperative, current.Cooperative)
	require.Equal(t, uint64(1), current.Offsets["orders"][0])
}

func TestCooperativeAdmissionRefreshComparesOwnership(t *testing.T) {
	c := cooperativeRecoveryCoordinator(t, cooperativeGroupFixture(t))
	installed := c.groups["workers"]
	copy := cloneGroupLifecycle(installed)
	require.True(t, sameConsumerSession(installed, copy))
	copy.Cooperative.Revision++
	require.False(t, sameConsumerSession(installed, copy))
	copy = cloneGroupLifecycle(installed)
	copy.Cooperative.Current[0].Epoch++
	require.False(t, sameConsumerSession(installed, copy))
}

func TestCooperativeCleanupScanIncludesOwnershipRevision(t *testing.T) {
	c := cooperativeRecoveryCoordinator(t, cooperativeGroupFixture(t))
	refs := c.topicGroupReferences("orders")
	scan := c.newTopicCleanupScan("orders", refs)
	require.True(t, c.cleanupTargetsCurrent(scan, refs))
	c.mu.Lock()
	c.groups["workers"].Cooperative.Revision++
	c.mu.Unlock()
	require.False(t, c.cleanupTargetsCurrent(scan, refs), "unchanged eager generation must not reuse stale cooperative evidence")
}

func TestCooperativeMultiTopicRecoveryProjection(t *testing.T) {
	partitions := []CooperativePartition{{Topic: "orders", Partition: 0, LifecycleEpoch: 7}, {Topic: "payments", Partition: 0, LifecycleEpoch: 9}}
	ownership, err := NewCooperativeAssignment(19, partitions)
	require.NoError(t, err)
	ownership = cooperativeJoin(t, cooperativeJoin(t, ownership, "a"), "b")
	snapshot := &GroupStateSnapshot{
		TopicName: subscriptionDisplayName([]string{"orders", "payments"}, ""),
		Topics:    []string{"orders", "payments"}, TopicPartitions: []TopicPartition{{Topic: "orders", Partition: 0}, {Topic: "payments", Partition: 0}},
		RegistrationEpoch: 19, Members: map[string][]int{"a": {}, "b": {}},
		TopicAssignments: map[string][]TopicPartition{"a": {{Topic: "orders", Partition: 0}, {Topic: "payments", Partition: 0}}},
		LastActivity:     time.Unix(100, 0).UTC(), Cooperative: ownership,
	}
	c := cooperativeRecoveryCoordinator(t, snapshot)
	var record ConsumerMetadataRecord
	c.SetOffsetRecordWriter(func(r ConsumerMetadataRecord) error { record = r; return nil })
	require.NoError(t, c.writeGroupLifecycleSnapshot("workers", 19, c.groups["workers"]))
	message := encodedMetadataMessage(t, record, 0)
	decoded, versioned, err := DecodeConsumerMetadataRecord(message.Payload)
	require.NoError(t, err)
	require.True(t, versioned)
	require.Equal(t, ownership, decoded.Lifecycle.Cooperative)
	require.ErrorIs(t, c.RegisterGroupSubscription("workers", snapshot.Topics, "", map[string]int{"orders": 1, "payments": 1}), errCooperativeProtocolRequired)
	snapshot.TopicAssignments["a"][1].Topic = "orders"
	require.Error(t, ValidateImportState(map[string]*GroupStateSnapshot{"workers": snapshot}))
}

func TestCooperativeStrictReplayPublishesAtomically(t *testing.T) {
	record := cooperativeLifecycleRecord(t)
	registration := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 6, Epoch: 19, Timestamp: time.Unix(90, 0).UTC()}
	c := newAuthorityTestCoordinator(t)
	proof := &ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*GroupStateSnapshot{}, SourcePartitions: map[string][]int{"workers": {0}}}
	require.NoError(t, c.ImportStateWithRecovery(nil, proof))
	acquireReplay(t, c)
	reader := strictReader(c)
	reader.messages[0] = []types.Message{encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, record, 1)}
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	before := c.ExportState()
	require.Equal(t, record.Lifecycle.Cooperative, before["workers"].Cooperative)
	conflicting := canonicalConsumerMetadataRecord(record)
	conflicting.Lifecycle.LastActivity = conflicting.Lifecycle.LastActivity.Add(time.Second)
	reader.messages[0] = append(reader.messages[0], encodedMetadataMessage(t, conflicting, 2))
	_, err = c.PrepareFullConsumerMetadataReplay(reader)
	require.ErrorContains(t, err, "conflicting cooperative lifecycle revision")
	require.Equal(t, before, c.ExportState())
}
