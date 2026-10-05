package transaction

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReservationCleanupCheckpointRemainsRecoverableUntilApplied(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "abort"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			m := NewManager()
			const id = "reserved-input"
			producer, epoch, err := m.InitProducerWithMode(id, ModeProcessingV1)
			require.NoError(t, err)
			require.NoError(t, m.Begin(id, producer, epoch))
			offsets := []OffsetOperation{{Topic: "input", Group: "workers", Member: "worker", Generation: 1, RegistrationEpoch: 1, Partition: 0, Offset: 9}}
			require.NoError(t, m.AddOffsets(id, producer, epoch, offsets))
			frozen, err := m.BeginOffsetReservations(id, producer, epoch)
			require.NoError(t, err)
			repeated, err := m.BeginOffsetReservations(id, producer, epoch)
			require.NoError(t, err)
			require.Equal(t, frozen, repeated)
			require.Error(t, m.AddOffsets(id, producer, epoch, offsets))
			require.Error(t, m.Abort(id, producer, epoch))
			_, _, err = m.InitProducerWithMode(id, ModeProcessingV1)
			require.Error(t, err)
			var decision *Snapshot
			if commit {
				_, err = m.PrepareCommit(id, producer, epoch)
				require.NoError(t, err)
				decision, err = m.BuildCommittedSnapshot(id)
			} else {
				decision, err = m.BuildAbortedSnapshot(id, producer, epoch)
			}
			require.NoError(t, err)
			require.NoError(t, m.ApplyReplicatedSnapshot(decision))
			first, err := m.BuildOffsetReservationsResolvedSnapshot(id)
			require.NoError(t, err)
			second, err := m.BuildOffsetReservationsResolvedSnapshot(id)
			require.NoError(t, err)
			firstBytes, err := json.Marshal(first)
			require.NoError(t, err)
			secondBytes, err := json.Marshal(second)
			require.NoError(t, err)
			require.Equal(t, firstBytes, secondBytes, "lost checkpoint ACK must retry an identical revision")
			require.Zero(t, m.PruneExpired(time.Now().Add(365*24*time.Hour)))
			pending, _ := m.PreparedTransactions(nil, 10)
			require.Len(t, pending, 1, "building a checkpoint must not remove pending recovery")
			require.True(t, pending[0].OffsetReservationsPending)
			require.NoError(t, m.ApplyReplicatedSnapshot(first))
			require.NoError(t, m.ApplyReplicatedSnapshot(second), "same checkpoint retry must be idempotent")
			pending, _ = m.PreparedTransactions(nil, 10)
			require.Empty(t, pending)
			resolved, err := m.Status(id)
			require.NoError(t, err)
			require.False(t, resolved.OffsetReservationsPending)
			require.Empty(t, resolved.Offsets)
			require.Equal(t, commit, resolved.OffsetsMaterialized)
			_, nextEpoch, err := m.InitProducerWithMode(id, ModeProcessingV1)
			require.NoError(t, err)
			require.Equal(t, epoch+1, nextEpoch)
		})
	}
}
