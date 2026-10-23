package fsm

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRecoveryQuantumAllowsOneOversizedRecordAndResumes(t *testing.T) {
	f, _ := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: strings.Repeat("a", 16384)}, {Offset: 1, Payload: "small"}})
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: 2, MaxRecords: MaxReplicaCatchupRecords, MaxBytes: 4096}
	b, err := f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	require.Len(t, b.Messages, 1)
	require.EqualValues(t, 1, b.EndOffset)
	require.False(t, b.Verified)
	require.NoError(t, AdvanceReplicaCatchupRequest(&r, b))
	b, err = f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	require.Len(t, b.Messages, 1)
	require.EqualValues(t, 1, b.Messages[0].Offset)
	require.True(t, b.Verified)
}
