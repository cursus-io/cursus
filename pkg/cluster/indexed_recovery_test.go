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
	expected.Verified = false
	expected, err = fsm.SealReplicaCatchupBatch(expected)
	require.NoError(t, err)
	encoded, err := json.Marshal(expected)
	require.NoError(t, err)
	fitted, err := fitReplicaCatchupBatchToLimit(batch, len(encoded))
	require.NoError(t, err)
	require.Equal(t, prefix, *fitted.MatchedPrefix)
	require.Equal(t, uint64(3), fitted.EndOffset)
	require.False(t, fitted.Verified)
	require.NoError(t, fsm.ValidateReplicaRecoveryMode(fitted))
	require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(fitted))
}

func TestFitRetainedRecoveryBatchPreservesExactChallenge(t *testing.T) {
	prefix := disk.RetainedPrefix{Version: 1, End: 1, Digest: strings.Repeat("a", 64)}
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{StartOffset: 1, EndOffset: 5, CommittedHWM: 5, Verified: true,
		RecoveryProtocol: 2, RetainedPrefix: &prefix, MatchedRetainedPrefix: &prefix,
		Messages: []types.Message{{Offset: 1, Payload: "one"}, {Offset: 2, Payload: "two"}, {Offset: 3, Payload: "three"}, {Offset: 4, Payload: "four"}}})
	require.NoError(t, err)
	expected := batch
	expected.Messages, expected.EndOffset, expected.Verified = expected.Messages[:2], 3, false
	expected, err = fsm.SealReplicaCatchupBatch(expected)
	require.NoError(t, err)
	encoded, err := json.Marshal(expected)
	require.NoError(t, err)
	fitted, err := fitReplicaCatchupBatchToLimit(batch, len(encoded))
	require.NoError(t, err)
	encoded, err = json.Marshal(fitted)
	require.NoError(t, err)
	var decoded fsm.ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, prefix, *decoded.RetainedPrefix)
	require.Equal(t, prefix, *decoded.MatchedRetainedPrefix)
	require.Equal(t, uint64(3), decoded.EndOffset)
	require.False(t, decoded.Verified)
	require.NoError(t, fsm.ValidateReplicaRecoveryMode(decoded))
	require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(decoded))
}
