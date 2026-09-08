package transaction

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalShardStoreFiltersAndValidatesShards(t *testing.T) {
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.journal"))
	require.NoError(t, err)
	store, err := NewJournalShardStore(journal, 4)
	require.NoError(t, err)

	firstID := transactionIDForShard(t, 1, 4)
	secondID := transactionIDForShard(t, 2, 4)
	first := &Snapshot{ID: firstID, State: StateCommitted}
	second := &Snapshot{ID: secondID, State: StateAborted}
	require.NoError(t, store.Persist(1, first))
	require.NoError(t, store.Persist(2, second))
	require.Error(t, store.Persist(3, first))

	loaded, err := store.Load([]int{1})
	require.NoError(t, err)
	require.Contains(t, loaded, firstID)
	require.NotContains(t, loaded, secondID)

	empty, err := store.Load([]int{})
	require.NoError(t, err)
	require.Empty(t, empty)
}
