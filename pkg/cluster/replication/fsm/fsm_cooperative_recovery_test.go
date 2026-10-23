package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/stretchr/testify/require"
)

func TestCooperativeSnapshotSelectsVersionAndRestoresOwnership(t *testing.T) {
	ownership, err := coordinator.NewCooperativeAssignment(19, []coordinator.CooperativePartition{
		{Topic: "orders", Partition: 0, LifecycleEpoch: 7}, {Topic: "orders", Partition: 1, LifecycleEpoch: 7},
	})
	require.NoError(t, err)
	ownership, err = ownership.Join("a", nil)
	require.NoError(t, err)
	ownership, err = ownership.Join("b", nil)
	require.NoError(t, err)
	groups := map[string]*coordinator.GroupStateSnapshot{"workers": {
		TopicName: "orders", Partitions: []int{0, 1}, RegistrationEpoch: 19,
		Members: map[string][]int{"a": {0, 1}, "b": {}}, LastActivity: time.Unix(100, 0).UTC(),
		Offsets: map[string]map[int]uint64{}, Cooperative: ownership,
	}}
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &registrationActivityReader{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportState(groups))
	before := c.ExportState()
	snapshot, err := NewBrokerFSM(nil, c).Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	var encoded bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &encoded}))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(encoded.Bytes(), &state))
	require.Equal(t, SnapshotVersionCooperative, state.Version)
	require.Equal(t, ownership, state.GroupState["workers"].Cooperative)
	restored, err := coordinator.NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &registrationActivityReader{})
	require.NoError(t, err)
	t.Cleanup(restored.Stop)
	require.NoError(t, NewBrokerFSM(nil, restored).Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	require.Equal(t, before, restored.ExportState())
	for _, version := range []int{SnapshotVersionLegacyEpoch, SnapshotVersionProducerEpoch, SnapshotVersionCurrent} {
		state.Version = version
		data, marshalErr := json.Marshal(state)
		require.NoError(t, marshalErr)
		require.ErrorIs(t, NewBrokerFSM(nil, restored).Restore(io.NopCloser(bytes.NewReader(data))), ErrUnsupportedRecoveryProtocol)
		require.Equal(t, before, restored.ExportState(), "old-format failure must be atomic")
	}
	t.Logf("raft_snapshot_bytes=%d grants=%d pending_revocations=1", encoded.Len(), len(ownership.Current))
}

func TestEagerSnapshotKeepsExistingVersion(t *testing.T) {
	s := &BrokerFSMSnapshot{groupState: map[string]*coordinator.GroupStateSnapshot{"workers": {RegistrationEpoch: 19}}}
	require.Equal(t, SnapshotVersionCurrent, s.writeVersion())
	require.Equal(t, 11, s.writeVersion())
}
