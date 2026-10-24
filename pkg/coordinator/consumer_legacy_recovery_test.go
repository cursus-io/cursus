package coordinator

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type certifiedLegacyReader struct {
	*strictReplayReader
	validate func() error
}

func (r *certifiedLegacyReader) ValidateFullCommittedCoverage() error { return r.validate() }

func TestFencedLegacyRecoveryPreservesUnknownAndRejectsStalePublication(t *testing.T) {
	for _, mutation := range []string{"none", "coverage", "expired coverage", "restore", "lease", "authority", "corrupt", "wrong key"} {
		t.Run(mutation, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("events", "keep", 1))
			before := c.ExportState()
			require.NoError(t, c.ImportStateWithRecovery(before, nil))
			release := acquireReplay(t, c)
			reader := &certifiedLegacyReader{strictReplayReader: strictReader(c), validate: func() error { return nil }}
			switch mutation {
			case "coverage":
				reader.validate = func() error { return errors.New("missing committed coverage") }
			case "corrupt":
				reader.messages[0] = []types.Message{{Payload: `{"version":`}}
			case "wrong key":
				m := encodedMetadataMessage(t, replayRegistration("keep", 1), 0)
				m.Key = "wrong"
				reader.messages[0] = []types.Message{m}
			}
			prepared, err := c.PrepareFencedLegacyConsumerMetadataReplay(reader)
			if mutation == "coverage" || mutation == "corrupt" || mutation == "wrong key" {
				require.Error(t, err)
				require.Equal(t, before, c.ExportState())
				return
			}
			require.NoError(t, err)
			switch mutation {
			case "expired coverage":
				reader.validate = func() error { return errors.New("coverage view expired") }
			case "restore":
				generation := c.BeginConsumerRestore()
				c.FinishConsumerRestore(generation, nil)
			case "lease":
				release()
				acquireReplay(t, c)
			case "authority":
				require.NoError(t, c.RegisterGroupFromFSM("events", "later", 1))
			}
			beforePublish := c.ExportState()
			err = c.ApplyFullConsumerMetadataReplayAndComplete(prepared)
			if mutation != "none" {
				require.Error(t, err)
				require.Equal(t, beforePublish, c.ExportState())
			} else {
				require.NoError(t, err)
				require.Equal(t, before, c.ExportState())
			}
			require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
		})
	}
}

func TestFencedLegacyReplayReadsShortPagesAndRespectsNewRegistrationEpoch(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.ImportStateWithRecovery(nil, nil))
	release := acquireReplay(t, c)
	reader := &certifiedLegacyReader{strictReplayReader: strictReader(c), validate: func() error { return nil }}
	records := []types.Message{
		encodedMetadataMessage(t, replayRegistration("g", 1), 0),
		encodedMetadataMessage(t, replayOffset("g", 1, 1, 100), 1),
		encodedMetadataMessage(t, replayRegistration("g", 2), 2),
		encodedMetadataMessage(t, replayOffset("g", 2, 1, 7), 3),
	}
	reader.end = map[int]uint64{0: 4}
	reader.read = func(p int, offset uint64, _ int) ([]types.Message, error) {
		if p != 0 || offset >= uint64(len(records)) {
			return nil, nil
		}
		return records[offset : offset+1], nil
	}
	prepared, err := c.PrepareFencedLegacyConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	release()
	offset, found := c.GetOffset("g", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(7), offset)
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
}

func TestFencedLegacyReplayValidatesAcknowledgedSnapshotRevision(t *testing.T) {
	for _, fault := range []string{"none", "ahead", "conflict", "regression", "missing", "new epoch"} {
		t.Run(fault, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
			snapshot := c.ExportState()
			snapshot["g"].Offsets = map[string]map[int]uint64{"events": {0: 100}}
			snapshot["g"].OffsetRevisions = map[string]uint64{"events": 2}
			require.NoError(t, c.ImportStateWithRecovery(snapshot, nil))
			before := c.ExportState()
			acquireReplay(t, c)
			reader := &certifiedLegacyReader{strictReplayReader: strictReader(c), validate: func() error { return nil }}
			epoch, revision, offset := snapshot["g"].RegistrationEpoch, uint64(2), uint64(100)
			switch fault {
			case "ahead":
				revision = 1
			case "conflict":
				offset = 99
			case "regression":
				revision, offset = 3, 99
			case "new epoch":
				epoch++
				revision, offset = 1, 7
			}
			if fault != "missing" {
				reader.messages[0] = []types.Message{
					encodedMetadataMessage(t, replayRegistration("g", epoch), 0),
					encodedMetadataMessage(t, replayOffset("g", epoch, revision, offset), 1),
				}
			}
			prepared, err := c.PrepareFencedLegacyConsumerMetadataReplay(reader)
			if fault == "none" || fault == "new epoch" {
				require.NoError(t, err)
				require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
				require.Equal(t, offset, c.ExportState()["g"].Offsets["events"][0])
			} else {
				require.Error(t, err)
				require.Equal(t, before, c.ExportState())
			}
		})
	}
}

func TestFencedLegacyReplayRejectsConflictingReservationDecision(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	snapshot := c.ExportState()
	snapshot["g"].ReservationRevision = 1
	snapshot["g"].ReservationDecisions = map[string]OffsetReservationDecision{
		"txn": {ProducerID: "producer", ProducerEpoch: 1, Committed: true},
	}
	require.NoError(t, c.ImportStateWithRecovery(snapshot, nil))
	before := c.ExportState()
	acquireReplay(t, c)
	reader := &certifiedLegacyReader{strictReplayReader: strictReader(c), validate: func() error { return nil }}
	epoch := snapshot["g"].RegistrationEpoch
	reader.messages[0] = []types.Message{
		encodedMetadataMessage(t, replayRegistration("g", epoch), 0),
		encodedMetadataMessage(t, ConsumerMetadataRecord{
			Version: ConsumerMetadataRecordVersionReservations, Type: ConsumerMetadataRecordOffsetReservations,
			Group: "g", Epoch: epoch, Revision: 1,
			ReservationDecisions: map[string]OffsetReservationDecision{"txn": {ProducerID: "producer", ProducerEpoch: 1, Committed: false}},
		}, 1),
	}
	_, err := c.PrepareFencedLegacyConsumerMetadataReplay(reader)
	require.ErrorContains(t, err, "reservation revision conflicts")
	require.Equal(t, before, c.ExportState())
}
