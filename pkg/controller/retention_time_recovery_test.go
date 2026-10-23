package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRetentionTimeSurvivesVerifiedCatchupBoundary(t *testing.T) {
	handler, _, partition, batch := indexedApplyFixture(t)
	proofs, err := partition.ReplicaLineageCandidates(2)
	require.NoError(t, err)
	batch.MatchedPrefix, batch.TruncateTo = &proofs[0], nil
	batch.EndOffset, batch.Verified = batch.CommittedHWM, true
	stamp := time.Unix(1700000000, 0).UnixMilli()
	batch.Messages = []types.Message{{Offset: 2, Payload: "recovered", LeaderEpoch: 8, AppendTimeMS: stamp}}
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	encoded, err := json.Marshal(batch)
	require.NoError(t, err)
	var received fsm.ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal(encoded, &received))
	require.Equal(t, stamp, received.Messages[0].AppendTimeMS)
	// Timestamp provenance is bound by the existing recovery digest, not just
	// carried as unchecked JSON alongside the authoritative record.
	received.Messages[0].AppendTimeMS++
	require.ErrorContains(t, handler.ApplyReplicaCatchup(context.Background(), received), "checksum")
	require.Equal(t, uint64(2), partition.NextOffset())
	received.Messages[0].AppendTimeMS = stamp
	require.NoError(t, handler.ApplyReplicaCatchup(context.Background(), received))
	require.Equal(t, uint64(3), partition.GetHWM())
	records, err := partition.ReadMessages(2, 1)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, stamp, records[0].AppendTimeMS)
}
