package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func newAuthorityTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c := NewCoordinator(context.Background(), cfg, &countingPublisher{})
	t.Cleanup(c.Stop)
	return c
}

func TestConsumerRecoveryHistoricalAuthorityExcludesModernOverlay(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	writes := 0
	c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		writes++
		c.RecordConsumerMetadataSource(record.Group, 3)
		return nil
	})
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 2))
	require.NoError(t, c.JoinGroupFromFSM("g", "m", "events", 2))
	require.NoError(t, c.ApplyFencedOffsetUpdateFromFSM("g", "events", "m", 1, []OffsetItem{{Partition: 0, Offset: 5}}))
	require.Zero(t, writes)
	require.NoError(t, c.CommitOffset("g", "events", 0, 100))
	require.NoError(t, c.PrepareOffsetReservation("g", 1, TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "m", Generation: 1, Offsets: []ReservedTransactionOffset{{Topic: "events", Partition: 1, Offset: 9}}}))
	// A lifecycle command applied after the larger disk overlay must not clone it.
	require.NoError(t, c.RemoveConsumerForGenerationFromFSM("g", "m", 1))
	live, envelope, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.True(t, envelope.KnownComplete)
	require.Equal(t, uint64(100), live["g"].Offsets["events"][0])
	require.Equal(t, uint64(5), envelope.RaftCompatibilityState["g"].Offsets["events"][0])
	require.Empty(t, envelope.RaftCompatibilityState["g"].OffsetRevisions)
	require.Empty(t, envelope.RaftCompatibilityState["g"].OffsetReservations)
	require.NotEmpty(t, live["g"].OffsetReservations)
	require.Equal(t, []int{3}, envelope.SourcePartitions["g"])
	before := envelope.RaftCompatibilityState
	require.Error(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 6}}))
	_, after, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, before, after.RaftCompatibilityState)
	require.True(t, after.KnownComplete)
}

func TestConsumerRecoveryHistoricalSubscriptionAndImplicitJoinNeverPublish(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error {
		t.Error("historical replay published")
		return errors.New("unexpected publish")
	})
	require.NoError(t, c.JoinGroupFromFSM("implicit", "m", "events", 2))
	require.NoError(t, c.ExpireConsumersFromFSM("implicit", 1, []string{"m"}))
	require.NoError(t, c.RegisterGroupSubscriptionFromFSM("multi", []string{"a", "b"}, "", map[string]int{"a": 1, "b": 1}))
	require.NoError(t, c.JoinGroupFromFSM("multi", "m", "", 0))
	require.NoError(t, c.ApplyFencedTopicOffsetUpdateFromFSM("multi", "m", 1, 1, map[string][]OffsetItem{"a": {{Partition: 0, Offset: 2}}, "b": {{Partition: 0, Offset: 3}}}))
	require.NoError(t, c.RemoveConsumerForGenerationFromFSM("multi", "m", 1))
	_, envelope, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.True(t, envelope.KnownComplete)
	require.Equal(t, 2, envelope.RaftCompatibilityState["implicit"].Generation)
	require.Equal(t, uint64(3), envelope.RaftCompatibilityState["multi"].Offsets["b"][0])
}

func TestConsumerRecoveryUnprovableApplyMakesUnknownSticky(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroup("events", "modern", 1))
	_, err := c.AddConsumer("modern", "m")
	require.NoError(t, err)
	require.NoError(t, c.ApplyFencedOffsetUpdateFromFSM("modern", "events", "m", 1, []OffsetItem{{Partition: 0, Offset: 5}}))
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
	require.NoError(t, c.RegisterGroupFromFSM("events", "later", 1))
	_, envelope, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.False(t, envelope.KnownComplete)
	require.Empty(t, envelope.RaftCompatibilityState)
}

func TestConsumerRecoveryPairValidationDetachAndMissingEnvelope(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ValidateConsumerMetadataBootstrapEligibility(0))
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	c.RecordConsumerMetadataSource("deleted", 3)
	groups, envelope, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	restored := newAuthorityTestCoordinator(t)
	require.NoError(t, restored.ImportStateWithRecovery(groups, envelope))
	groups["g"].Partitions[0] = 99
	envelope.RaftCompatibilityState["g"].Partitions[0] = 99
	envelope.SourcePartitions["deleted"][0] = 99
	actual, proof, err := restored.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, []int{0}, actual["g"].Partitions)
	require.Equal(t, []int{0}, proof.RaftCompatibilityState["g"].Partitions)
	require.Equal(t, []int{3}, proof.SourcePartitions["deleted"])
	for _, mutate := range []func(*ConsumerRecoveryEnvelope){
		func(e *ConsumerRecoveryEnvelope) { e.Version++ },
		func(e *ConsumerRecoveryEnvelope) { e.KnownComplete = false },
		func(e *ConsumerRecoveryEnvelope) { e.RaftCompatibilityState = nil },
		func(e *ConsumerRecoveryEnvelope) { e.SourcePartitions["bad"] = []int{-1} },
		func(e *ConsumerRecoveryEnvelope) { e.SourcePartitions["bad"] = []int{1, 1} },
		func(e *ConsumerRecoveryEnvelope) { e.RaftCompatibilityState["g"].RegistrationEpoch = 0 },
	} {
		_, bad, err := restored.ExportStateWithRecovery()
		require.NoError(t, err)
		mutate(bad)
		require.Error(t, restored.ImportStateWithRecovery(nil, bad))
		unchanged, unchangedProof, err := restored.ExportStateWithRecovery()
		require.NoError(t, err)
		require.Equal(t, actual, unchanged)
		require.Equal(t, proof, unchangedProof)
	}
	require.Error(t, restored.ImportStateWithRecovery(map[string]*GroupStateSnapshot{"bad": nil}, proof))
	require.NoError(t, restored.ImportState(nil))
	require.Empty(t, restored.ExportState())
	require.ErrorIs(t, restored.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
	_, unknown, err := restored.ExportStateWithRecovery()
	require.NoError(t, err)
	require.False(t, unknown.KnownComplete)
	require.NoError(t, restored.ImportStateWithRecovery(nil, unknown))
	require.NoError(t, restored.LoadOffsetsFromLog(&metadataReplayHandler{}))
	require.NoError(t, restored.RegisterGroupFromFSM("events", "new", 1))
	require.ErrorIs(t, restored.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
}

func TestConsumerRecoveryHistoricalShellBaselineRoundTrip(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("shell", "events", []OffsetItem{{Partition: 0, Offset: 7}}))
	// Modern registration completes only the live shell.
	c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error { c.RecordConsumerMetadataSource(record.Group, 0); return nil })
	require.NoError(t, c.RegisterGroup("events", "shell", 1))
	groups, proof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.True(t, proof.KnownComplete)
	require.Zero(t, proof.RaftCompatibilityState["shell"].RegistrationEpoch)
	require.Error(t, ValidateImportState(proof.RaftCompatibilityState))
	peer := newAuthorityTestCoordinator(t)
	require.NoError(t, peer.ImportStateWithRecovery(groups, proof))
	require.NoError(t, peer.RegisterGroupFromFSM("events", "shell", 1))
	_, proof, err = peer.ExportStateWithRecovery()
	require.NoError(t, err)
	require.True(t, proof.KnownComplete)
	require.Equal(t, uint64(7), proof.RaftCompatibilityState["shell"].Offsets["events"][0])
}

func TestConsumerRecoveryProvenanceIncludesDiscardedAndDeletedNames(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	registration := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration, Group: "g", Topic: "events", PartitionCount: 1, Epoch: 2}
	old := registration
	old.Epoch = 1
	tombstone := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordTombstone, Group: "gone", Topic: "events", Epoch: 3}
	orphan := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot, Group: "orphan", Topic: "events", Epoch: 1, Revision: 1, Offsets: []OffsetItem{{Partition: 0, Offset: 9}}}
	handler := &metadataReplayHandler{messages: map[int][]types.Message{
		0: {encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, tombstone, 1)},
		2: {encodedMetadataMessage(t, old, 0), encodedMetadataMessage(t, orphan, 1)},
		3: {{Offset: 0, Payload: `{"group":"legacy","topic":"events","partition":0,"offset":5}`}},
	}}
	require.NoError(t, c.LoadOffsetsFromLog(handler))
	_, proof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, proof.SourcePartitions["g"])
	require.Equal(t, []int{0}, proof.SourcePartitions["gone"])
	require.Equal(t, []int{2}, proof.SourcePartitions["orphan"])
	require.Equal(t, []int{3}, proof.SourcePartitions["legacy"])
	require.Empty(t, proof.RaftCompatibilityState)
	require.NoError(t, c.LoadOffsetsFromLog(&metadataReplayHandler{}))
	_, after, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, proof.SourcePartitions, after.SourcePartitions)
}

func TestConsumerRecoveryOpaqueWritesLoseProofOnlyOnWrite(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return errors.New("failed append") })
	require.NoError(t, c.ValidateConsumerMetadataBootstrapEligibility(0))
	require.Error(t, c.RegisterGroup("events", "g", 1))
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
	c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error { c.RecordConsumerMetadataSource(record.Group, 2); return nil })
	require.NoError(t, c.RegisterGroup("events", "g", 1))
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
}

func TestConsumerRecoverySnapshotWriteProvenanceConcurrency(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		c.RecordConsumerMetadataSource(record.Group, int(record.Offsets[0].Offset))
		return nil
	})
	var wg sync.WaitGroup
	wg.Add(1)
	failures := make(chan error, 1)
	go func() {
		defer wg.Done()
		for n := 1; n <= 100; n++ {
			if err := c.CommitOffset("g", "events", 0, uint64(n)); err != nil {
				failures <- err
				return
			}
		}
	}()
	for n := 0; n < 100; n++ {
		groups, proof, err := c.ExportStateWithRecovery()
		require.NoError(t, err)
		if value := groups["g"].Offsets["events"][0]; value > 0 {
			require.Contains(t, proof.SourcePartitions["g"], int(value), fmt.Sprint(value))
		}
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		c.RecordConsumerMetadataSource(record.Group, 999)
		return errors.New("failed")
	})
	require.Error(t, c.CommitOffset("g", "events", 0, 101))
	groups, proof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, uint64(100), groups["g"].Offsets["events"][0])
	require.Contains(t, proof.SourcePartitions["g"], 999)
}

func TestConsumerRecoveryUnroutedOrdinaryMutationsLoseProof(t *testing.T) {
	for _, mutation := range []struct {
		name  string
		apply func(*Coordinator) error
	}{
		{"registration", func(c *Coordinator) error { return c.RegisterGroup("events", "modern", 1) }},
		{"single", func(c *Coordinator) error { return c.CommitOffset("g", "events", 0, 5) }},
		{"bulk", func(c *Coordinator) error {
			return c.CommitOffsetsBulk("g", "events", []OffsetItem{{Partition: 0, Offset: 5}})
		}},
		{"multi", func(c *Coordinator) error {
			return c.ValidateAndCommitTopicOffsetsBulk("g", "m", 1, map[string][]OffsetItem{"events": {{Partition: 0, Offset: 5}}})
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.JoinGroupFromFSM("g", "m", "events", 1))
			require.NoError(t, c.ValidateConsumerMetadataBootstrapEligibility(0))
			require.NoError(t, mutation.apply(c))
			require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
		})
	}
}

func TestConsumerRecoveryImportDoesNotWaitForLifecyclePublisher(t *testing.T) {
	for _, scenario := range []struct {
		name                                string
		existing, subscription, emptyImport bool
	}{
		{"new registration replaced", false, false, false},
		{"new registration empty import", false, false, true},
		{"new subscription empty import", false, true, true},
		{"existing lifecycle replaced", true, false, false},
		{"existing lifecycle empty import", true, false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			receiver := newAuthorityTestCoordinator(t)
			if scenario.existing {
				require.NoError(t, receiver.RegisterGroupFromFSM("events", "g", 1))
			}
			incoming := newAuthorityTestCoordinator(t)
			if !scenario.emptyImport {
				require.NoError(t, incoming.RegisterGroupFromFSM("events", "g", 2))
				incoming.RecordConsumerMetadataSource("g", 1)
			}
			groups, proof, err := incoming.ExportStateWithRecovery()
			require.NoError(t, err)
			awaitingRaft := make(chan struct{})
			releaseRaft := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseRaft) }) }
			defer release()
			receiver.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
				receiver.RecordConsumerMetadataSource(record.Group, 3)
				close(awaitingRaft)
				// The writer holds lifecycleMu but has released c.mu. This models its
				// pending PARTITION_COMMIT while Restore owns the FSM transition lock.
				<-releaseRaft
				return nil
			})
			writeDone := make(chan error, 1)
			go func() {
				if scenario.existing {
					_, err := receiver.AddConsumer("g", "m")
					writeDone <- err
				} else if scenario.subscription {
					writeDone <- receiver.RegisterGroupSubscription("g", []string{"events"}, "", map[string]int{"events": 1})
				} else {
					writeDone <- receiver.RegisterGroup("events", "g", 1)
				}
			}()
			select {
			case <-awaitingRaft:
			case <-time.After(5 * time.Second):
				t.Fatal("lifecycle publisher did not reach simulated Raft")
			}
			importDone := make(chan error, 1)
			go func() { importDone <- receiver.ImportStateWithRecovery(groups, proof) }()
			select {
			case err := <-importDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("paired import waited for lifecycle publisher awaiting Raft")
			}
			imported, importedProof, err := receiver.ExportStateWithRecovery()
			require.NoError(t, err)
			require.Equal(t, groups, imported)
			require.Equal(t, proof, importedProof)
			release()
			select {
			case err := <-writeDone:
				require.ErrorContains(t, err, "changed during durable")
			case <-time.After(5 * time.Second):
				t.Fatal("lifecycle publisher did not complete")
			}
			after, afterProof, err := receiver.ExportStateWithRecovery()
			require.NoError(t, err)
			require.Equal(t, groups, after)
			require.Equal(t, proof, afterProof)
		})
	}
}

func TestConsumerRecoveryImportFencesReplayScannedBeforeImport(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	scanned, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	handler := &pausedMetadataReplayHandler{
		metadataReplayHandler: metadataReplayHandler{messages: map[int][]types.Message{0: {encodedMetadataMessage(t, ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration, Group: "old", Topic: "events", PartitionCount: 1, Epoch: 1}, 0)}}},
		scanned:               scanned, resume: resume,
	}
	replayDone := make(chan error, 1)
	go func() { replayDone <- c.LoadOffsetsFromLog(handler) }()
	select {
	case <-scanned:
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not pause after source scan")
	}
	incoming := newAuthorityTestCoordinator(t)
	groups, proof, err := incoming.ExportStateWithRecovery()
	require.NoError(t, err)
	require.NoError(t, c.ImportStateWithRecovery(groups, proof))
	release()
	select {
	case err := <-replayDone:
		require.ErrorContains(t, err, "state imported during replay")
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not finish")
	}
	after, afterProof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, groups, after)
	require.Equal(t, proof, afterProof)
}

type authorityMigrationHandler struct {
	metadataReplayHandler
	records       []ConsumerMetadataRecord
	authoritative bool
}

func (h *authorityMigrationHandler) ConsumerMetadataMigrationRecords() ([]ConsumerMetadataRecord, bool, error) {
	return h.records, h.authoritative, nil
}
func TestConsumerRecoveryMigrationCannotEstablishProvenance(t *testing.T) {
	for _, test := range []struct {
		name                             string
		withRecord, authoritative, known bool
	}{
		{"nonempty", true, false, false},
		{"nonempty authoritative", true, true, false},
		{"empty authoritative", false, true, false},
		{"empty nonauthoritative", false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &authorityMigrationHandler{authoritative: test.authoritative}
			if test.withRecord {
				handler.records = []ConsumerMetadataRecord{{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration, Group: "migrated", Topic: "events", PartitionCount: 1, Epoch: 1}}
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			c, err := NewCoordinatorWithRecovery(context.Background(), cfg, handler)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			if test.withRecord {
				require.NotNil(t, c.GetGroup("migrated"))
			}
			for n := 0; n < 2; n++ {
				_, proof, err := c.ExportStateWithRecovery()
				require.NoError(t, err)
				require.Equal(t, test.known, proof.KnownComplete)
				require.Equal(t, test.known, proof.SourcePartitionsKnown)
				require.True(t, proof.RaftCompatibilityKnown)
				require.Empty(t, proof.RaftCompatibilityState)
				if test.known {
					require.NoError(t, c.ValidateConsumerMetadataBootstrapEligibility(0))
				} else {
					require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
				}
				require.NoError(t, c.ReloadDistributedConsumerMetadata())
			}
		})
	}
}
