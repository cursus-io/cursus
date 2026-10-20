package replication

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestVoterAdmissionIndexUsesLatestLogOrCompactedSnapshot(t *testing.T) {
	logs, snapshots := raft.NewInmemStore(), raft.NewInmemSnapshotStore()
	first := raft.Configuration{Servers: []raft.Server{{ID: "one", Address: "one:1", Suffrage: raft.Voter}}}
	second := first.Clone()
	second.Servers = append(second.Servers, raft.Server{ID: "two", Address: "two:1", Suffrage: raft.Voter})
	require.NoError(t, logs.StoreLogs([]*raft.Log{
		{Index: 1, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(first)},
		{Index: 2, Type: raft.LogCommand},
		{Index: 3, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(second)},
		{Index: 4, Type: raft.LogCommand},
	}))
	_, err := voterAdmissionIndex(logs, snapshots, first)
	require.ErrorContains(t, err, "changed")
	index, err := voterAdmissionIndex(logs, snapshots, second)
	require.NoError(t, err)
	require.EqualValues(t, 3, index)
	_, transport := raft.NewInmemTransport("one:1")
	defer func() { require.NoError(t, transport.Close()) }()
	sink, err := snapshots.Create(raft.SnapshotVersionMax, 4, 1, second, 3, transport)
	require.NoError(t, err)
	_, err = io.WriteString(sink, "snapshot")
	require.NoError(t, err)
	require.NoError(t, sink.Close())
	require.NoError(t, logs.DeleteRange(1, 4))
	index, err = voterAdmissionIndex(logs, snapshots, second)
	require.NoError(t, err)
	require.EqualValues(t, 3, index)
	_, err = voterAdmissionIndex(logs, snapshots, first)
	require.Error(t, err)
	_, err = voterAdmissionIndex(raft.NewInmemStore(), raft.NewInmemSnapshotStore(), first)
	require.Error(t, err)
}
