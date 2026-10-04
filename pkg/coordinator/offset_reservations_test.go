package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func newReservationCoordinator(t *testing.T) (*Coordinator, TransactionOffsetReservation, uint64) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	t.Cleanup(c.Stop)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	require.NoError(t, c.RegisterGroup("orders", "workers", 2))
	_, err := c.AddConsumer("workers", "worker")
	require.NoError(t, err)
	reservation := TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: c.GetGeneration("workers"),
		Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 0, Offset: 10}}}
	return c, reservation, c.GetRegistrationEpoch("workers")
}

func TestOffsetReservationSurvivesMemberDepartureUntilDecision(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "commit"}[committed], func(t *testing.T) {
			c, reservation, epoch := newReservationCoordinator(t)
			require.NoError(t, c.CommitOffset("workers", "orders", 0, 3))
			require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation))
			_, _, err := c.GetStableOffset("workers", "orders", 0)
			require.ErrorContains(t, err, "unstable_offset_commit")
			require.ErrorContains(t, c.CommitOffset("workers", "orders", 0, 12), "unstable_offset_commit")
			require.ErrorContains(t, c.CommitOffsetsBulk("workers", "orders", []OffsetItem{{Partition: 0, Offset: 12}}), "unstable_offset_commit")
			require.NoError(t, c.CommitOffset("workers", "orders", 1, 7), "unrelated partitions must remain available")
			require.NoError(t, c.RemoveConsumerForGeneration("workers", "worker", reservation.Generation))
			require.ErrorContains(t, c.DeleteGroup("workers"), "pending transaction")
			require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation), "retry uses durable authorization, not current membership")
			require.ErrorContains(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 1, committed), "fenced")
			require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, committed))
			offset, found, err := c.GetStableOffset("workers", "orders", 0)
			require.NoError(t, err)
			require.True(t, found)
			want := uint64(3)
			if committed {
				want = 10
			}
			require.Equal(t, want, offset)
			require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, committed))
			require.NoError(t, c.DeleteGroup("workers"))
			require.NoError(t, c.RegisterGroup("orders", "workers", 2))
			require.ErrorContains(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true), "group_epoch_mismatch")
			_, found, err = c.GetStableOffset("workers", "orders", 0)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestOffsetReservationLostAppendResponseCannotReuseRevision(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	var written []ConsumerMetadataRecord
	fail := true
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		written = append(written, record)
		if fail {
			fail = false
			return fmt.Errorf("append acknowledgement lost")
		}
		return nil
	})
	require.ErrorContains(t, c.PrepareOffsetReservation("workers", epoch, reservation), "acknowledgement lost")
	require.Len(t, c.GetGroup("workers").OffsetReservations, 1)
	_, _, unstable := c.GetStableOffset("workers", "orders", 0)
	require.ErrorContains(t, unstable, "unstable_offset_commit", "an ambiguous prepare must retain the input fence")
	require.NoError(t, c.RemoveConsumerForGeneration("workers", "worker", reservation.Generation))
	require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, false))
	require.Equal(t, uint64(1), written[0].Revision)
	last := written[len(written)-1]
	require.Equal(t, uint64(2), last.Revision)
	require.Empty(t, last.Reservations, "abort must supersede a possibly durable failed prepare")
	candidates := newConsumerMetadataCandidates()
	status := ConsumerMetadataRecoveryStatus{}
	for _, record := range written {
		require.NoError(t, candidates.selectRecord(record, &status))
	}
	require.Empty(t, candidates.reservationSnapshots["workers"].record.Reservations)
}

func TestOffsetReservationCommitFailureKeepsStableReadsFenced(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation))
	var revisions []uint64
	fail := true
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordOffsetSnapshot {
			revisions = append(revisions, record.Revision)
			if fail {
				fail = false
				return fmt.Errorf("offset append response lost")
			}
		}
		return nil
	})
	require.ErrorContains(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true), "response lost")
	_, _, err := c.GetStableOffset("workers", "orders", 0)
	require.ErrorContains(t, err, "unstable_offset_commit")
	require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true))
	require.Equal(t, []uint64{1, 2}, revisions)
	offset, found, err := c.GetStableOffset("workers", "orders", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(10), offset)
}

func TestOffsetReservationReleaseFailureRetainsFenceAndNewerOffset(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation))
	// Recovery may already have installed a more advanced committed snapshot.
	group := c.GetGroup("workers")
	group.mu.Lock()
	group.storeOffset("orders", 0, 20)
	group.mu.Unlock()
	failRelease := true
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordOffsetSnapshot {
			require.Equal(t, uint64(20), record.Offsets[0].Offset)
		}
		if record.Type == ConsumerMetadataRecordOffsetReservations && failRelease {
			failRelease = false
			return fmt.Errorf("release append unavailable")
		}
		return nil
	})
	require.ErrorContains(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true), "release append unavailable")
	_, _, err := c.GetStableOffset("workers", "orders", 0)
	require.ErrorContains(t, err, "unstable_offset_commit")
	require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true))
	offset, found, err := c.GetStableOffset("workers", "orders", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(20), offset)
}

func TestOffsetReservationPrepareSerializesWithRebalance(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	entered, release := make(chan struct{}), make(chan struct{})
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordOffsetReservations {
			close(entered)
			<-release
		}
		return nil
	})
	prepared := make(chan error, 1)
	go func() { prepared <- c.PrepareOffsetReservation("workers", epoch, reservation) }()
	<-entered
	rebalanced := make(chan error, 1)
	go func() { _, err := c.AddConsumer("workers", "replacement"); rebalanced <- err }()
	select {
	case err := <-rebalanced:
		close(release)
		t.Fatalf("rebalance passed uncommitted reservation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-prepared)
	require.NoError(t, <-rebalanced)
	_, _, err := c.GetStableOffset("workers", "orders", 0)
	require.ErrorContains(t, err, "unstable_offset_commit")
}

func TestOffsetReservationReloadCannotDiscardNewerPrepareOrRelease(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	registration := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", Epoch: epoch, PartitionCount: 2, Timestamp: time.Now().UTC()}
	lifecycle := ConsumerMetadataRecord{Version: 4, Type: ConsumerMetadataRecordLifecycleSnapshot, Group: "workers", Epoch: epoch, Revision: uint64(reservation.Generation), Lifecycle: lifecycleSnapshot(c.GetGroup("workers"))}
	reader := &metadataReplayHandler{messages: map[int][]types.Message{0: {encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, lifecycle, 1)}}}
	c.topicHandler = reader
	require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation))
	require.NoError(t, c.ReloadDistributedConsumerMetadata())
	_, _, err := c.GetStableOffset("workers", "orders", 0)
	require.ErrorContains(t, err, "unstable_offset_commit", "a scan started before the prepare must retain its acknowledged fence")
	prepared := ConsumerMetadataRecord{Version: 5, Type: ConsumerMetadataRecordOffsetReservations, Group: "workers", Epoch: epoch, Revision: 1, Reservations: []TransactionOffsetReservation{reservation}}
	reader.messages[0] = append(reader.messages[0], encodedMetadataMessage(t, prepared, 2))
	require.NoError(t, c.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, false))
	require.NoError(t, c.ReloadDistributedConsumerMetadata())
	_, _, err = c.GetStableOffset("workers", "orders", 0)
	require.NoError(t, err, "a stale scan must not resurrect a released reservation")
	require.Equal(t, uint64(2), c.GetGroup("workers").ReservationRevision)
}

func reservationFixture() ConsumerMetadataRecord {
	return ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersionReservations, Type: ConsumerMetadataRecordOffsetReservations,
		Group: "workers", Epoch: 3, Revision: 1, Timestamp: time.Unix(10, 0).UTC(),
		Reservations: []TransactionOffsetReservation{{TransactionalID: "tx-a", ProducerID: "producer", ProducerEpoch: 2, MemberID: "departed", Generation: 4,
			Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 1, Offset: 20}, {Topic: "orders", Partition: 0, Offset: 10}}}},
	}
}

func TestOffsetReservationSnapshotCanonicalEncodingAndValidation(t *testing.T) {
	record := reservationFixture()
	payload, key, err := encodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	decoded, _, err := DecodeConsumerMetadataRecord(string(payload))
	require.NoError(t, err)
	require.Equal(t, canonicalConsumerMetadataRecord(record), decoded)
	require.Equal(t, 1, record.Reservations[0].Offsets[0].Partition, "canonicalization must not mutate the caller")
	cleared := record
	cleared.Revision++
	cleared.Reservations = nil
	_, clearKey, err := encodeConsumerMetadataRecord(cleared)
	require.NoError(t, err)
	require.Equal(t, key, clearKey, "a release snapshot must replace its reservation snapshot during compaction")
	require.NotEqual(t, consumerMetadataRecordKey(ConsumerMetadataRecord{Group: record.Group, Type: ConsumerMetadataRecordLifecycleSnapshot}), key)
	for name, mutate := range map[string]func(*ConsumerMetadataRecord){
		"wrong version":         func(r *ConsumerMetadataRecord) { r.Version = 4 },
		"missing revision":      func(r *ConsumerMetadataRecord) { r.Revision = 0 },
		"wrong type":            func(r *ConsumerMetadataRecord) { r.Type = ConsumerMetadataRecordOffsetSnapshot },
		"mixed fields":          func(r *ConsumerMetadataRecord) { r.Topic = "orders" },
		"no producer":           func(r *ConsumerMetadataRecord) { r.Reservations[0].ProducerID = "" },
		"no offsets":            func(r *ConsumerMetadataRecord) { r.Reservations[0].Offsets = nil },
		"negative epoch":        func(r *ConsumerMetadataRecord) { r.Reservations[0].ProducerEpoch = -1 },
		"duplicate transaction": func(r *ConsumerMetadataRecord) { r.Reservations = append(r.Reservations, r.Reservations[0]) },
		"overlapping inputs": func(r *ConsumerMetadataRecord) {
			other := r.Reservations[0]
			other.TransactionalID = "tx-b"
			r.Reservations = append(r.Reservations, other)
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := reservationFixture()
			mutate(&invalid)
			_, _, err := encodeConsumerMetadataRecord(invalid)
			require.Error(t, err)
		})
	}
}

func TestOffsetReservationsRecoverAfterMembershipChangesAndRelease(t *testing.T) {
	registration := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", Epoch: 3, PartitionCount: 2, Timestamp: time.Unix(10, 0).UTC()}
	// The old member is already gone when the reservation is recovered.
	lifecycle := ConsumerMetadataRecord{Version: 4, Type: ConsumerMetadataRecordLifecycleSnapshot, Group: "workers", Epoch: 3, Revision: 5,
		Lifecycle: &GroupLifecycleSnapshot{TopicName: "orders", Generation: 5, Partitions: []int{0, 1}, Members: []GroupLifecycleMember{{ID: "replacement", Assignments: []int{0, 1}}}, LastActivity: time.Unix(11, 0).UTC()}}
	reservation := reservationFixture()
	for _, cleared := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepared", true: "released"}[cleared], func(t *testing.T) {
			records := []ConsumerMetadataRecord{reservation, lifecycle, registration}
			if cleared {
				release := reservation
				release.Revision = 2
				release.Reservations = nil
				records = append([]ConsumerMetadataRecord{release}, records...)
			}
			messages := make([]types.Message, 0, len(records))
			for i, record := range records {
				messages = append(messages, encodedMetadataMessage(t, record, uint64(i)))
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			c, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			require.Equal(t, 5, c.GetGeneration("workers"))
			group := c.GetGroup("workers")
			if cleared {
				require.Empty(t, group.OffsetReservations)
				require.Equal(t, uint64(2), group.ReservationRevision)
			} else {
				require.Equal(t, cloneOffsetReservations(reservation.Reservations), group.OffsetReservations)
			}
			state := c.ExportState()
			restored := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
			t.Cleanup(restored.Stop)
			require.NoError(t, restored.ImportState(state))
			require.Equal(t, group.OffsetReservations, restored.GetGroup("workers").OffsetReservations)
			if !cleared {
				state["workers"].OffsetReservations[0].Offsets[0].Offset = 999
				require.Equal(t, uint64(10), restored.GetGroup("workers").OffsetReservations[0].Offsets[0].Offset)
				require.Equal(t, uint64(10), group.OffsetReservations[0].Offsets[0].Offset)
			}
		})
	}
}

func TestDistributedRecoveryFailsClosedOnCorruptReservation(t *testing.T) {
	for _, corruption := range []string{"payload", "key", "legacy"} {
		t.Run(corruption, func(t *testing.T) {
			message := encodedMetadataMessage(t, reservationFixture(), 0)
			switch corruption {
			case "payload":
				message.Payload = "not-json"
			case "key":
				message.Key = "wrong-key"
			case "legacy":
				message.Payload = "{\"group\":\"workers\",\"topic\":\"orders\",\"partition\":0,\"offset\":1}"
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			c, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: {message}}})
			t.Cleanup(c.Stop)
			require.Error(t, err, "ignoring an unreadable reservation could expose a stale resume offset")
			require.False(t, c.RecoverySnapshot().Ready)
		})
	}
}

func TestOffsetReservationReplayRejectsConflictsAndFencesGroupIncarnations(t *testing.T) {
	record := reservationFixture()
	candidates := newConsumerMetadataCandidates()
	status := ConsumerMetadataRecoveryStatus{}
	require.NoError(t, candidates.selectRecord(record, &status))
	conflict := reservationFixture()
	conflict.Reservations[0].Offsets[0].Offset++
	require.ErrorContains(t, candidates.selectRecord(conflict, &status), "conflicting")
	groups := map[string]*GroupMetadata{"workers": {TopicName: "orders", Partitions: []int{0, 1}, RegistrationEpoch: 4}}
	orphans, err := restoreOffsetReservations(groups, candidates.reservationSnapshots)
	require.NoError(t, err)
	require.Equal(t, 1, orphans)
	require.Empty(t, groups["workers"].OffsetReservations)
	groups["workers"].RegistrationEpoch = 3
	groups["workers"].Partitions = []int{0}
	_, err = restoreOffsetReservations(groups, candidates.reservationSnapshots)
	require.ErrorContains(t, err, "undeclared")
}
