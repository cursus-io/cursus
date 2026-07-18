package replication

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestDurableRaftStoreSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := newDurableRaftStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.Set([]byte("term"), []byte("7")))
	require.NoError(t, store.StoreLog(&raft.Log{
		Index: 3,
		Term:  7,
		Type:  raft.LogCommand,
		Data:  []byte("PARTITION_COMMIT:test"),
	}))
	require.NoError(t, store.Close())

	reopened, err := newDurableRaftStore(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	term, err := reopened.Get([]byte("term"))
	require.NoError(t, err)
	require.Equal(t, []byte("7"), term)

	var log raft.Log
	require.NoError(t, reopened.GetLog(3, &log))
	require.Equal(t, uint64(7), log.Term)
	require.Equal(t, []byte("PARTITION_COMMIT:test"), log.Data)
}
