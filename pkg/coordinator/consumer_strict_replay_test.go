package coordinator

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type strictReplayReader struct {
	messages map[int][]types.Message
	count    int
	first    map[int]uint64
	end      map[int]uint64
	read     func(int, uint64, int) ([]types.Message, error)
}

func (r *strictReplayReader) ExistingPartitionCount(string) (int, error) { return r.count, nil }
func (r *strictReplayReader) EarliestTopicOffset(_ string, p int) (uint64, error) {
	return r.first[p], nil
}
func (r *strictReplayReader) EndTopicOffset(_ string, p int) (uint64, error) {
	if end, ok := r.end[p]; ok {
		return end, nil
	}
	var end uint64
	for _, m := range r.messages[p] {
		if m.Offset >= end {
			end = m.Offset + 1
		}
	}
	return end, nil
}
func (r *strictReplayReader) ReadTopicPartition(_ string, p int, offset uint64, max int) ([]types.Message, error) {
	if r.read != nil {
		return r.read(p, offset, max)
	}
	var out []types.Message
	for _, m := range r.messages[p] {
		if m.Offset >= offset && len(out) < max {
			out = append(out, m)
		}
	}
	return out, nil
}

func replayRegistration(name string, epoch uint64) ConsumerMetadataRecord {
	return ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration, Group: name, Topic: "events", Epoch: epoch, PartitionCount: 1}
}
func replayOffset(name string, epoch, revision, offset uint64) ConsumerMetadataRecord {
	return ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot, Group: name, Topic: "events", Epoch: epoch, Revision: revision, Offsets: []OffsetItem{{Partition: 0, Offset: offset}}}
}
func acquireReplay(t *testing.T, c *Coordinator) func() {
	t.Helper()
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	t.Cleanup(release)
	return release
}

func TestStrictReplayTombstonePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		epoch        uint64
		registration bool
		wantLive     bool
	}{
		{"empty", 0, false, false}, {"older registration", 4, true, false}, {"same registration", 5, true, true}, {"newer registration", 6, true, true},
		{"older offset", 4, false, false}, {"same offset", 5, false, false}, {"newer offset", 6, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			proof := &ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*GroupStateSnapshot{"g": {Deleted: true, RegistrationEpoch: 5}}, SourcePartitions: map[string][]int{"g": {0}}}
			require.NoError(t, c.ImportStateWithRecovery(nil, proof))
			acquireReplay(t, c)
			reader := strictReader(c)
			reader.messages[0] = []types.Message{{Offset: 0, Payload: `{"group":"g","topic":"events","partition":0,"offset":100}`}}
			if tc.epoch != 0 {
				record := replayOffset("g", tc.epoch, 1, 7)
				if tc.registration {
					record = replayRegistration("g", tc.epoch)
				}
				reader.messages[0] = append(reader.messages[0], encodedMetadataMessage(t, record, 1))
			}
			prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
			require.NoError(t, err)
			require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
			require.Equal(t, tc.wantLive, c.groups["g"] != nil)
			if !tc.wantLive {
				require.Equal(t, uint64(5), c.groupEpochs["g"])
			}
		})
	}
}

func TestStrictReplayScopedUnionAndUnrelatedIdentity(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	// Historical state establishes a valid baseline; overlay is deliberately newer.
	for _, name := range []string{"target", "unrelated"} {
		require.NoError(t, c.RegisterGroupFromFSM("events", name, 1))
		require.NoError(t, c.JoinGroupFromFSM(name, "member", "events", 1))
	}
	c.SetRoutedOffsetRecordWriter(func(r ConsumerMetadataRecord) error { c.RecordConsumerMetadataSource(r.Group, 0); return nil })
	require.NoError(t, c.CommitOffset("target", "events", 0, 100))
	require.NoError(t, c.CommitOffset("target", "events", 0, 101))
	require.NoError(t, c.PrepareOffsetReservation("target", 1, TransactionOffsetReservation{TransactionalID: "old", ProducerID: "p", ProducerEpoch: 1, MemberID: "member", Generation: 1, Offsets: []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 200}}}))
	require.NoError(t, c.PrepareOffsetReservation("unrelated", 1, TransactionOffsetReservation{TransactionalID: "keep", ProducerID: "p", ProducerEpoch: 1, MemberID: "member", Generation: 1, Offsets: []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 9}}}))
	pointer := c.groups["unrelated"]
	heartbeat := pointer.Members["member"].LastHeartbeat
	before := c.ExportState()["unrelated"]
	when := time.Now().Add(-time.Hour)
	c.ownershipSince["target"] = when
	c.ownershipSince["unrelated"] = when
	c.groupEpochs["routed-deleted"] = 8
	c.RecordConsumerMetadataSource("compacted", 3)
	c.groups["compacted"] = &GroupMetadata{RegistrationEpoch: 9}
	c.groups["old-tail"] = &GroupMetadata{RegistrationEpoch: 9}
	c.groups["new-tail"] = &GroupMetadata{RegistrationEpoch: 9}
	c.groupEpochs["old-tail"] = 9
	c.groupEpochs["new-tail"] = 9
	acquireReplay(t, c)
	route := func(name string) (int, error) {
		if name == "target" || name == "routed-deleted" {
			return 3, nil
		}
		return 1, nil
	}
	old := strictReader(c)
	old.messages[3] = []types.Message{encodedMetadataMessage(t, replayRegistration("old-tail", 1), 0)}
	scope, err := c.CaptureConsumerMetadataScope(3, old, route)
	require.NoError(t, err)
	committed := strictReader(c)
	committed.messages[0] = []types.Message{encodedMetadataMessage(t, replayRegistration("target", 1), 0), encodedMetadataMessage(t, replayOffset("target", 1, 1, 7), 1)}
	replacement := strictReader(c)
	replacement.messages[3] = []types.Message{encodedMetadataMessage(t, replayRegistration("new-tail", 1), 0)}
	prepared, err := c.PrepareScopedConsumerMetadataReplay(committed, replacement, scope, route)
	require.NoError(t, err)
	require.Error(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.NoError(t, c.ApplyScopedConsumerMetadataReplay(prepared))
	require.Equal(t, uint64(7), c.groups["target"].Offsets["events"][0])
	require.Equal(t, uint64(1), c.groups["target"].OffsetRevisions["events"])
	require.Empty(t, c.groups["target"].OffsetReservations)
	require.Zero(t, c.groups["target"].ReservationRevision)
	for _, name := range []string{"compacted", "old-tail", "new-tail", "routed-deleted"} {
		require.Nil(t, c.groups[name])
		_, exists := c.groupEpochs[name]
		require.False(t, exists)
	}
	require.Same(t, pointer, c.groups["unrelated"])
	require.Equal(t, before, c.ExportState()["unrelated"])
	require.Equal(t, heartbeat, c.groups["unrelated"].Members["member"].LastHeartbeat)
	require.Equal(t, when, c.ownershipSince["unrelated"])
	_, exists := c.ownershipSince["target"]
	require.False(t, exists)
}

func TestStrictReplayRejectsStalePreparedResults(t *testing.T) {
	for _, kind := range []string{"lease", "historical", "import", "other install", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
			release := acquireReplay(t, c)
			prepared, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
			require.NoError(t, err)
			switch kind {
			case "lease":
				release()
				acquireReplay(t, c)
			case "historical":
				require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 9}}))
			case "import":
				groups, proof, err := c.ExportStateWithRecovery()
				require.NoError(t, err)
				require.NoError(t, c.ImportStateWithRecovery(groups, proof))
			case "other install":
				other, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
				require.NoError(t, err)
				require.NoError(t, c.ApplyFullConsumerMetadataReplay(other))
			case "foreign":
				c = newAuthorityTestCoordinator(t)
				acquireReplay(t, c)
			}
			before := c.ExportState()
			require.Error(t, c.ApplyFullConsumerMetadataReplay(prepared))
			require.Equal(t, before, c.ExportState())
		})
	}
}

func TestStrictReplayRejectsInvalidRecordsAtomically(t *testing.T) {
	mismatch := encodedMetadataMessage(t, replayRegistration("g", 1), 0)
	mismatch.Key = "wrong"
	for _, message := range []types.Message{{Payload: `{"version":`}, mismatch, {Payload: `{}`}, {Payload: `{"group":"g","topic":"events","partition":-1,"offset":7}`}} {
		t.Run(message.Payload+message.Key, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("events", "keep", 1))
			before := c.ExportState()
			acquireReplay(t, c)
			reader := strictReader(c)
			reader.messages[0] = []types.Message{message}
			_, err := c.PrepareFullConsumerMetadataReplay(reader)
			require.Error(t, err)
			_, err = c.CaptureConsumerMetadataScope(0, reader, func(string) (int, error) { return 0, nil })
			require.Error(t, err)
			require.Equal(t, before, c.ExportState())
			// Distributed ordinary loading deliberately retains its tolerant behavior.
			require.NoError(t, c.LoadOffsetsFromLog(reader))
		})
	}
}

func TestStrictRawScopeRejectsLegacyReservationKey(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	raw := strictReader(c)
	raw.messages[0] = []types.Message{{Key: "cursus.consumer.offset-reservations.bad", Payload: `{"group":"g","topic":"events","partition":0,"offset":1}`}}
	_, err := c.CaptureConsumerMetadataScope(0, raw, func(string) (int, error) { return 0, nil })
	require.Error(t, err)
}

func TestStrictReplayReaderBoundaries(t *testing.T) {
	for _, kind := range []string{"partial", "reversed", "outside", "overflow", "duplicate", "oversize", "read error"} {
		t.Run(kind, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			acquireReplay(t, c)
			reader := strictReader(c)
			reader.end = map[int]uint64{0: 10}
			switch kind {
			case "partial":
				reader.count--
			case "reversed":
				reader.first = map[int]uint64{0: 11}
			case "outside":
				reader.messages[0] = []types.Message{{Offset: 10}}
			case "overflow":
				reader.end[0] = math.MaxUint64
				reader.messages[0] = []types.Message{{Offset: math.MaxUint64}}
			case "duplicate":
				reader.messages[0] = []types.Message{{Offset: 0, TransactionMarker: types.TransactionMarkerCommit}, {Offset: 0}}
			case "oversize":
				reader.read = func(int, uint64, int) ([]types.Message, error) { return make([]types.Message, 1025), nil }
			case "read error":
				reader.read = func(int, uint64, int) ([]types.Message, error) { return nil, fmt.Errorf("reader failed") }
			}
			_, err := c.PrepareFullConsumerMetadataReplay(reader)
			require.Error(t, err)
			require.Empty(t, c.groups)
		})
	}
}

func TestStrictReplayAllPartitionsAndOwnStart(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 7}}))
	acquireReplay(t, c)
	reader := strictReader(c)
	reader.first = map[int]uint64{0: 5, 1: 20}
	reader.end = map[int]uint64{0: 8, 1: 22}
	reader.messages[0] = []types.Message{{Offset: 5, Payload: `{"group":"g","topic":"events","partition":0,"offset":3}`}}
	reader.messages[1] = []types.Message{{Offset: 20, Payload: `{"group":"g","topic":"events","partition":1,"offset":9}`}}
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.Equal(t, map[int]uint64{0: 7, 1: 9}, c.groups["g"].Offsets["events"])
	require.Zero(t, c.groups["g"].RegistrationEpoch)
}

func TestStrictReplayScopeSurvivesImport(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	route := func(string) (int, error) { return 1, nil }
	old := strictReader(c)
	old.messages[0] = []types.Message{encodedMetadataMessage(t, replayRegistration("old", 1), 0)}
	scope, err := c.CaptureConsumerMetadataScope(0, old, route)
	require.NoError(t, err)
	proof := &ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*GroupStateSnapshot{}, SourcePartitions: map[string][]int{}}
	require.NoError(t, c.ImportStateWithRecovery(nil, proof))
	prepared, err := c.PrepareScopedConsumerMetadataReplay(strictReader(c), strictReader(c), scope, route)
	require.NoError(t, err)
	require.NoError(t, c.ApplyScopedConsumerMetadataReplay(prepared))
	_, after, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, []int{0}, after.SourcePartitions["old"])
}

func TestStrictReplayDetachedBaselineAfterPairedImport(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 4}}))
	c.SetRoutedOffsetRecordWriter(func(r ConsumerMetadataRecord) error { c.RecordConsumerMetadataSource(r.Group, 0); return nil })
	require.NoError(t, c.CommitOffset("g", "events", 0, 99))
	groups, proof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	peer := newAuthorityTestCoordinator(t)
	require.NoError(t, peer.ImportStateWithRecovery(groups, proof))
	acquireReplay(t, peer)
	peer.SetRoutedOffsetRecordWriter(func(ConsumerMetadataRecord) error { t.Fatal("scratch invoked writer"); return nil })
	peer.SetGroupSessionCallbacks(func(string) bool { t.Fatal("scratch invoked ownership callback"); return false }, nil)
	prepared, err := peer.PrepareFullConsumerMetadataReplay(strictReader(peer))
	require.NoError(t, err)
	require.Nil(t, prepared.scratch.topicHandler)
	require.Nil(t, prepared.scratch.transactionalOffsets)
	require.Nil(t, prepared.scratch.cancel)
	require.Nil(t, prepared.scratch.offsetRecordWriter)
	require.NoError(t, peer.ApplyFullConsumerMetadataReplay(prepared))
	require.Equal(t, uint64(4), peer.groups["g"].Offsets["events"][0])
	require.Equal(t, uint64(99), groups["g"].Offsets["events"][0])
	groups["g"].Offsets["events"][0] = 500
	proof.RaftCompatibilityState["g"].Offsets["events"][0] = 600
	require.Equal(t, uint64(4), peer.groups["g"].Offsets["events"][0])
}

func TestStrictReplayUnknownAuthorityAndChangesDuringRead(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	require.NoError(t, c.ImportState(nil))
	reader := strictReader(c)
	reader.read = func(int, uint64, int) ([]types.Message, error) {
		t.Fatal("unknown authority read log")
		return nil, nil
	}
	_, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.ErrorIs(t, err, ErrConsumerMetadataBootstrapUnknown)
	peer := newAuthorityTestCoordinator(t)
	acquireReplay(t, peer)
	reader = strictReader(peer)
	reader.end = map[int]uint64{0: 1}
	reader.read = func(int, uint64, int) ([]types.Message, error) {
		require.NoError(t, peer.RegisterGroupFromFSM("events", "new", 1))
		return nil, nil
	}
	_, err = peer.PrepareFullConsumerMetadataReplay(reader)
	require.ErrorContains(t, err, "stale")
	require.NotNil(t, peer.groups["new"])
}

func TestStrictScopeExpansionRejectsChangedOperationStamp(t *testing.T) {
	for _, operation := range []string{"capture", "prepare"} {
		for _, callback := range []string{"raw", "route"} {
			for _, change := range []string{"lease", "historical", "import", "eligibility"} {
				t.Run(operation+"/"+callback+"/"+change, func(t *testing.T) {
					c := newAuthorityTestCoordinator(t)
					require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
					release := acquireReplay(t, c)
					route := func(string) (int, error) { return 0, nil }
					old, err := c.CaptureConsumerMetadataScope(0, strictReader(c), route)
					require.NoError(t, err)
					changed := false
					var expected map[string]*GroupStateSnapshot
					mutate := func() {
						if changed {
							return
						}
						changed = true
						switch change {
						case "lease":
							release()
							acquireReplay(t, c)
						case "historical":
							require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 9}}))
						case "import":
							groups, proof, err := c.ExportStateWithRecovery()
							require.NoError(t, err)
							require.NoError(t, c.ImportStateWithRecovery(groups, proof))
						case "eligibility":
							c.markConsumerMetadataSourcesUnknown()
						}
						expected = c.ExportState()
					}
					raw := strictReader(c)
					if callback == "raw" {
						raw.end = map[int]uint64{0: 1}
						raw.read = func(int, uint64, int) ([]types.Message, error) { mutate(); return nil, nil }
					} else {
						route = func(string) (int, error) { mutate(); return 0, nil }
					}
					if operation == "capture" {
						_, err = c.CaptureConsumerMetadataScope(0, raw, route)
					} else {
						var prepared *PreparedConsumerMetadataReplay
						prepared, err = c.PrepareScopedConsumerMetadataReplay(strictReader(c), raw, old, route)
						require.Nil(t, prepared)
					}
					require.True(t, changed)
					require.Error(t, err)
					require.Equal(t, expected, c.ExportState())
				})
			}
		}
	}
}

func TestStrictReplayEpochZeroYieldsToVersionedRegistration(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 99}}))
	acquireReplay(t, c)
	reader := strictReader(c)
	reader.messages[0] = []types.Message{encodedMetadataMessage(t, replayRegistration("g", 1), 0), encodedMetadataMessage(t, replayOffset("g", 1, 1, 3), 1)}
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.Equal(t, uint64(1), c.groups["g"].RegistrationEpoch)
	require.Equal(t, uint64(3), c.groups["g"].Offsets["events"][0])
}

func TestStrictReplayNewEpochDoesNotInheritHistoricalMembership(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	require.NoError(t, c.JoinGroupFromFSM("g", "old", "events", 1))
	acquireReplay(t, c)
	reader := strictReader(c)
	reader.messages[0] = []types.Message{encodedMetadataMessage(t, replayRegistration("g", 2), 0)}
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.Empty(t, c.groups["g"].Members)
	require.Zero(t, c.groups["g"].Generation)
}

func TestStrictReplayWaitingForAuthorityDoesNotHoldAdmission(t *testing.T) {
	for _, operation := range []string{"apply", "capture", "prepare"} {
		for _, lock := range []string{"authority", "coordinator", "provenance"} {
			t.Run(operation+"/"+lock, func(t *testing.T) {
				c := newAuthorityTestCoordinator(t)
				acquireReplay(t, c)
				prepared, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
				require.NoError(t, err)
				route := func(string) (int, error) { return 0, nil }
				scope, err := c.CaptureConsumerMetadataScope(0, strictReader(c), route)
				require.NoError(t, err)
				unlock := c.authorityMu.Unlock
				switch lock {
				case "authority":
					c.authorityMu.Lock()
				case "coordinator":
					c.mu.Lock()
					unlock = c.mu.Unlock
				case "provenance":
					c.provenanceMu.Lock()
					unlock = c.provenanceMu.Unlock
				}
				finished := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "apply":
						err = c.ApplyFullConsumerMetadataReplay(prepared)
					case "capture":
						_, err = c.CaptureConsumerMetadataScope(0, strictReader(c), route)
					case "prepare":
						_, err = c.PrepareScopedConsumerMetadataReplay(strictReader(c), strictReader(c), scope, route)
					}
					finished <- err
				}()
				// Exercise readiness repeatedly while publication is held behind authority.
				deadline := time.Now().Add(100 * time.Millisecond)
				blocked := false
				for time.Now().Before(deadline) {
					result := make(chan error, 1)
					go func() { result <- c.ConsumerOperationsReadinessError() }()
					select {
					case err := <-result:
						require.ErrorIs(t, err, ErrConsumerMetadataRecovering)
					case <-time.After(100 * time.Millisecond):
						blocked = true
					}
					if blocked {
						break
					}
				}
				unlock()
				require.NoError(t, <-finished)
				require.False(t, blocked, "waiting for state locks must not hold admission")
			})
		}
	}
}
func (r *strictReplayReader) ReadCommittedConsumerMetadata(topic string, p int, offset uint64, max int) ([]types.Message, error) {
	return r.ReadTopicPartition(topic, p, offset, max)
}
func strictReader(c *Coordinator) *strictReplayReader {
	return &strictReplayReader{count: c.offsetTopicPartitionCount, messages: map[int][]types.Message{}}
}

func TestStrictReplayExcludesLiveOverlayAndRequiresLease(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 5}}))
	c.SetRoutedOffsetRecordWriter(func(r ConsumerMetadataRecord) error { c.RecordConsumerMetadataSource(r.Group, 0); return nil })
	require.NoError(t, c.CommitOffset("g", "events", 0, 100))
	reader := strictReader(c)
	_, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.Error(t, err)
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.Equal(t, uint64(5), c.ExportState()["g"].Offsets["events"][0])
	require.Error(t, c.ApplyFullConsumerMetadataReplay(prepared))
}

func TestStrictReplayRetainsHistoricalEpochZeroShell(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []OffsetItem{{Partition: 0, Offset: 5}}))
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	prepared, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.NotNil(t, c.ExportState()["g"])
	require.Equal(t, uint64(5), c.ExportState()["g"].Offsets["events"][0])
}
