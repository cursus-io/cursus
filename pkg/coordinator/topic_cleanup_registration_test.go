package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type boundaryCleanupLog struct{ *cleanupTestLog }

func (l *boundaryCleanupLog) Partitions() []ConsumerCleanupPartition {
	p := l.cleanupTestLog.Partitions()
	p[0].TopicEpoch = 1
	return p
}
func (l *boundaryCleanupLog) WithConsumerCleanupView(fn func(ConsumerCleanupView) error) error {
	return fn(l)
}
func (l *boundaryCleanupLog) CheckConsumerCleanupProof(bounds []ConsumerCleanupPartition) error {
	if len(bounds) != 1 || bounds[0] != l.Partitions()[0] {
		return fmt.Errorf("proof changed")
	}
	return nil
}

func TestTruncateCleanupPreservesReregisteredGroup(t *testing.T) {
	for _, topicEpoch := range []uint64{2, 3, 99999} {
		for _, registrationEpoch := range []uint64{3, 99999} {
			t.Run(fmt.Sprintf("topic-%d/group-%d", topicEpoch, registrationEpoch), func(t *testing.T) {
				c, log := newCleanupFollower(t)
				c.topicHandler = &boundaryCleanupLog{log}
				old := registrationEpoch - 2
				log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: old}))
				boundary := &TopicCleanupBoundary{TopicEpoch: topicEpoch, MetadataEpoch: 1, Ends: []uint64{1}}
				log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "workers", Topic: "orders", Epoch: old + 1}))
				log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: registrationEpoch}))
				c.groups["workers"].RegistrationEpoch = registrationEpoch
				c.groupEpochs["workers"] = registrationEpoch
				c.groups["workers"].Offsets = map[string]map[int]uint64{"orders": {0: 17}}
				before := c.ExportState()
				deleted, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, boundary)
				require.NoError(t, err)
				require.Empty(t, deleted)
				require.Equal(t, before, c.ExportState(), "active members, offsets and registration must survive")
				_, err = c.CheckTopicGroupCleanup("orders", boundary)
				require.NoError(t, err)
				// A second truncate includes the new registration in its cut.
				next := boundary.Clone()
				next.TopicEpoch++
				next.Ends[0] = log.end
				_, err = c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, next)
				require.Error(t, err)
				_, err = c.CheckTopicGroupCleanup("orders", next)
				require.Error(t, err)
				log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "workers", Topic: "orders", Epoch: registrationEpoch + 1}))
				log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: registrationEpoch + 2}))
				c.groups["workers"].RegistrationEpoch, c.groupEpochs["workers"] = registrationEpoch+2, registrationEpoch+2
				before = c.ExportState()
				_, err = c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, next)
				require.NoError(t, err)
				_, err = c.CheckTopicGroupCleanup("orders", next)
				require.NoError(t, err)
				require.Equal(t, before, c.ExportState(), "repeated truncate preserves its own later registration")
			})
		}
	}
}

func TestTruncateCleanupMissingEvidenceFailsClosed(t *testing.T) {
	for _, mutation := range []string{"unknown_cut", "missing_tombstone", "registration_before_cut", "retained_prefix", "uncommitted_registration", "wrong_internal_epoch", "transactional_registration", "malformed"} {
		t.Run(mutation, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			c.topicHandler = &boundaryCleanupLog{log}
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
			b := &TopicCleanupBoundary{TopicEpoch: 2, MetadataEpoch: 1, Ends: []uint64{1}}
			if mutation != "missing_tombstone" {
				log.append(cleanupMessage(t, cleanupDeletion()))
			}
			r := cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3})
			if mutation == "transactional_registration" {
				r.TransactionalID = "transaction"
			}
			log.append(r)
			c.groups["workers"].RegistrationEpoch = 3
			c.groupEpochs["workers"] = 3
			switch mutation {
			case "unknown_cut":
				b.Ends = nil
			case "registration_before_cut":
				b.Ends[0] = log.end
			case "retained_prefix":
				log.start = 1
			case "uncommitted_registration":
				log.end--
			case "wrong_internal_epoch":
				b.MetadataEpoch++
			case "malformed":
				log.append(types.Message{Payload: "{"})
			}
			c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
			before := c.ExportState()
			_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
			require.Error(t, err)
			require.Equal(t, before, c.ExportState())
			_, err = c.CheckTopicGroupCleanup("orders", b)
			require.Error(t, err)
		})
	}
}

func TestTruncateCleanupFirstRegistrationWithoutPriorDeletionFailsClosed(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	c.groups["workers"].Members = make(map[string]*MemberMetadata)
	c.groups["workers"].Offsets = nil
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
	b := &TopicCleanupBoundary{TopicEpoch: 999, MetadataEpoch: 1, Ends: []uint64{0}}
	before := c.ExportState()
	// Even an empty local historical overlay is not replicated proof of absence.
	// Lock contention must not change the outcome or block Raft apply.
	for _, locked := range []bool{false, true} {
		if locked {
			c.authorityMu.Lock()
		}
		_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
		if locked {
			c.authorityMu.Unlock()
		}
		require.Error(t, err)
		_, err = c.CheckTopicGroupCleanup("orders", b)
		require.Error(t, err)
		require.Equal(t, before, c.ExportState())
	}
}

func TestTruncateCleanupPreservesEmptyReregistration(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
	log.append(cleanupMessage(t, cleanupDeletion()))
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3}))
	c.groups["workers"].RegistrationEpoch, c.groupEpochs["workers"] = 3, 3
	c.groups["workers"].Members = make(map[string]*MemberMetadata)
	c.groups["workers"].Offsets = map[string]map[int]uint64{"orders": {0: 0}}
	before := c.ExportState()
	b := &TopicCleanupBoundary{TopicEpoch: 2, MetadataEpoch: 1, Ends: []uint64{1}}
	_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.NoError(t, err)
	_, err = c.CheckTopicGroupCleanup("orders", b)
	require.NoError(t, err)
	require.Equal(t, before, c.ExportState())
	log.identity++
	_, err = c.CheckTopicGroupCleanup("orders", b)
	require.Error(t, err, "final apply must reject replaced log evidence")
}

func TestTruncateCleanupCannotUseEmptyOverlayDuringRecovery(t *testing.T) {
	c, _ := newCleanupFollower(t)
	delete(c.groups, "workers")
	b := &TopicCleanupBoundary{TopicEpoch: 2}
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	_, err = c.CheckTopicGroupCleanup("orders", b)
	require.Error(t, err, "an unavailable empty overlay cannot prove absence")
	release()
	_, err = c.CheckTopicGroupCleanup("orders", b)
	require.NoError(t, err)
	c.consumerAdmission.mu.Lock()
	_, err = c.CheckTopicGroupCleanup("orders", b)
	c.consumerAdmission.mu.Unlock()
	require.Error(t, err, "Raft apply must never wait for recovery admission")
}

func TestOrdinaryReplayDoesNotAttachOldMembershipToNewRegistration(t *testing.T) {
	c, _ := newCleanupFollower(t)
	c.groups["workers"].Generation = 14
	r := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3}
	h := &metadataReplayHandler{messages: map[int][]types.Message{0: {encodedMetadataMessage(t, r, 0)}}}
	_, err := c.recoverConsumerMetadata(h)
	require.NoError(t, err)
	status, err := c.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, "Dead", status.State)
	require.Zero(t, status.Generation)
	require.Zero(t, status.MemberCount)
	require.Equal(t, uint64(3), c.GetRegistrationEpoch("workers"))
}

func TestTruncateCleanupPreservedBatchesDoNotStarveLaterGroups(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	delete(c.groups, "workers")
	for i := 0; i < topicCleanupGroups*2+1; i++ {
		name := fmt.Sprintf("new-%03d", i)
		c.groups[name] = &GroupMetadata{TopicName: "orders", RegistrationEpoch: 3, Members: make(map[string]*MemberMetadata)}
		log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: name, Topic: "orders", Epoch: 2}))
		log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: name, Topic: "orders", PartitionCount: 1, Epoch: 3}))
	}
	b := &TopicCleanupBoundary{TopicEpoch: 888, MetadataEpoch: 1, Ends: []uint64{0}}
	var err error
	for i := 0; i < 30; i++ {
		_, err = c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
		if err == nil {
			break
		}
		var pending *TopicCleanupPending
		require.ErrorAs(t, err, &pending)
		require.Equal(t, CleanupContinue, pending.State)
	}
	require.NoError(t, err)
	_, err = c.CheckTopicGroupCleanup("orders", b)
	require.NoError(t, err)
	require.Len(t, c.groups, topicCleanupGroups*2+1)
}

type boundaryDurableLog struct{ *durableCleanupTestLog }

func (l *boundaryDurableLog) Partitions() []ConsumerCleanupPartition {
	p := l.durableCleanupTestLog.Partitions()
	p[0].TopicEpoch = 1
	return p
}
func (l *boundaryDurableLog) WithConsumerCleanupView(fn func(ConsumerCleanupView) error) error {
	return fn(l)
}
func (l *boundaryDurableLog) CheckConsumerCleanupProof(bounds []ConsumerCleanupPartition) error {
	if len(bounds) != 1 || bounds[0] != l.Partitions()[0] {
		return fmt.Errorf("proof changed")
	}
	return nil
}

func TestTruncateCleanupCheckpointBindsOperationAndRegistration(t *testing.T) {
	for _, mutation := range []string{"restart", "operation", "cut", "old_format", "registration", "generation"} {
		t.Run(mutation, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			c.cfg.LogDir = t.TempDir()
			reader := &boundaryDurableLog{&durableCleanupTestLog{log, "durable-generation"}}
			c.topicHandler = reader
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
			log.append(cleanupMessage(t, cleanupDeletion()))
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3}))
			for range 300 {
				log.append(types.Message{Payload: `{"group":"other"}`})
			}
			c.groups["workers"].RegistrationEpoch = 3
			c.groupEpochs["workers"] = 3
			b := &TopicCleanupBoundary{TopicEpoch: 3, MetadataEpoch: 1, Ends: []uint64{1}}
			_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
			require.Error(t, err)
			saved := c.cleanupProgress["orders"].next[0]
			require.Positive(t, saved)
			fresh, _ := newCleanupFollower(t)
			fresh.cfg.LogDir, fresh.topicHandler = c.cfg.LogDir, reader
			require.NoError(t, fresh.ImportState(c.ExportState()))
			switch mutation {
			case "operation":
				b.TopicEpoch++
			case "cut":
				b.Ends[0] = 3
			case "registration":
				fresh.groups["workers"].RegistrationEpoch = 5
			case "generation":
				reader.token = "replacement"
			case "old_format":
				path := c.cleanupCheckpointPath("orders")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				var e cleanupCheckpointEnvelope
				require.NoError(t, json.Unmarshal(data, &e))
				var cp cleanupCheckpoint
				require.NoError(t, json.Unmarshal(e.Payload, &cp))
				cp.Version = 1
				e.Payload, err = json.Marshal(cp)
				require.NoError(t, err)
				sum := sha256.Sum256(e.Payload)
				e.SHA256 = hex.EncodeToString(sum[:])
				data, err = json.Marshal(e)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			reads := len(log.reads)
			for i := 0; i < 10; i++ {
				_, err = fresh.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
				if err == nil {
					break
				}
			}
			if mutation == "restart" {
				require.Equal(t, saved, log.reads[reads])
				require.NoError(t, err)
			} else {
				require.Zero(t, log.reads[reads])
			}
			if mutation == "cut" || mutation == "registration" {
				require.Error(t, err)
			}
			require.NotNil(t, fresh.GetGroup("workers"))
		})
	}
}

func TestTruncateCleanupRegistrationChangesBetweenPasses(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
	b := &TopicCleanupBoundary{TopicEpoch: 2, MetadataEpoch: 1, Ends: []uint64{1}}
	for range 300 {
		log.append(types.Message{Payload: `{"group":"unrelated"}`})
	}
	_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.Error(t, err)
	log.append(cleanupMessage(t, cleanupDeletion()))
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3}))
	state := c.ExportState()
	state["workers"].RegistrationEpoch = 3
	require.NoError(t, c.ImportState(state))
	for i := 0; i < 10; i++ {
		_, err = c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.Equal(t, uint64(3), c.GetRegistrationEpoch("workers"))
	// Becoming owner after the proof must not originate a destructive tombstone.
	c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
	_, err = c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.NoError(t, err)
}

func TestTruncateCleanupRefreshesChangedMembershipTarget(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
	b := &TopicCleanupBoundary{TopicEpoch: 2, MetadataEpoch: 1, Ends: []uint64{1}}
	_, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.ErrorContains(t, err, "waiting for a newer committed group tombstone")
	group := c.GetGroup("workers")
	group.mu.Lock()
	group.Generation++ // Membership may change between bounded reconciliation passes.
	group.mu.Unlock()
	log.append(cleanupMessage(t, cleanupDeletion()))
	deleted, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, deleted)
	require.Nil(t, c.GetGroup("workers"))
}

func TestTruncateCleanupRemovesOnlyStaleLocalIncarnation(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.topicHandler = &boundaryCleanupLog{log}
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1}))
	b := &TopicCleanupBoundary{TopicEpoch: 99, MetadataEpoch: 1, Ends: []uint64{1}}
	log.append(cleanupMessage(t, cleanupDeletion()))
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 3}))
	log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordOffsetSnapshot, Group: "workers", Topic: "orders", Epoch: 3, Revision: 1, Offsets: []OffsetItem{{Partition: 0, Offset: 17}}}))
	before := append([]types.Message(nil), log.messages...)
	deleted, err := c.ReconcileTopicGroupsContext(context.Background(), "orders", func() bool { return true }, b)
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, deleted)
	require.Nil(t, c.GetGroup("workers"), "only the old local epoch is discarded")
	require.Equal(t, before, log.messages, "no deletion is appended for the new group")
	_, err = c.recoverConsumerMetadata(&metadataReplayHandler{messages: map[int][]types.Message{0: log.messages}})
	require.NoError(t, err)
	require.Equal(t, uint64(3), c.GetRegistrationEpoch("workers"))
	require.Equal(t, uint64(17), mustOffset(t, c, "workers", "orders", 0))
}
