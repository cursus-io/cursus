package cluster

import (
	"encoding/json"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFitIndexedRecoveryBatchPreservesMatchedPrefix(t *testing.T) {
	prefix := disk.LineagePrefix{EndOffset: 1, Digest: strings.Repeat("a", 64)}
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{StartOffset: 1, EndOffset: 5, CommittedHWM: 5, Verified: true, RecoveryProtocol: 1, MatchedPrefix: &prefix, Messages: []types.Message{{Offset: 1, Payload: "one"}, {Offset: 2, Payload: "two"}, {Offset: 3, Payload: "three"}, {Offset: 4, Payload: "four"}}})
	require.NoError(t, err)
	expected := batch
	expected.Messages = expected.Messages[:2]
	expected.EndOffset = 3
	expected, err = fsm.SealReplicaCatchupBatch(expected)
	require.NoError(t, err)
	encoded, err := json.Marshal(expected)
	require.NoError(t, err)
	fitted, err := fitReplicaCatchupBatchToLimit(batch, len(encoded))
	require.NoError(t, err)
	require.Equal(t, prefix, *fitted.MatchedPrefix)
	require.Equal(t, uint64(3), fitted.EndOffset)
	require.NoError(t, fsm.ValidateReplicaRecoveryMode(fitted))
	require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(fitted))
}
