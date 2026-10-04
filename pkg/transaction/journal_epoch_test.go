package transaction

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalEpochWatermarkSurvivesEmptyRewriteAndTailRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	// Import a version-one journal before the first upgrade/compaction.
	legacy, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	require.NoError(t, err)
	snap := testJournalSnapshot("retired", 1, StateCommitted)
	snap.Epoch = 81
	_, err = writeJournalRecord(legacy, journalRecord{Version: 1, Transaction: snap})
	require.NoError(t, err)
	require.NoError(t, legacy.Close())
	state, err := j.Load()
	require.NoError(t, err)
	require.Len(t, state, 1)
	require.Equal(t, uint64(82), j.NextProducerEpoch())
	require.NoError(t, j.Rewrite(nil))
	for range 3 {
		j, err = OpenJournal(path)
		require.NoError(t, err)
		state, err = j.Load()
		require.NoError(t, err)
		require.Empty(t, state)
		require.Equal(t, uint64(82), j.NextProducerEpoch())
		inspection, err := InspectJournal(path)
		require.NoError(t, err)
		require.Equal(t, 1, inspection.RecordCount, "bounded metadata after all IDs expire")
		require.Zero(t, inspection.LatestTransactions)
		require.NoError(t, j.Rewrite(nil))
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.Write([]byte{0, 0, 1})
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = j.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(82), j.NextProducerEpoch())
	snap.Epoch = 0
	require.NoError(t, j.Append(snap))
	require.NoError(t, j.Rewrite(nil))
	_, err = j.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(82), j.NextProducerEpoch(), "older snapshots cannot rewind the watermark")
}

func TestJournalCorruptSoleWatermarkFailsWithoutTruncating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	require.NoError(t, j.Append(testJournalSnapshot("retired", 1, StateCommitted)))
	require.NoError(t, j.Rewrite(nil))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)-1] ^= 1
	require.NoError(t, os.WriteFile(path, data, 0o600))
	_, err = j.Load()
	require.ErrorContains(t, err, "checksum mismatch")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, after)
}

func TestJournalRejectsInvalidWatermarkRecords(t *testing.T) {
	for _, payload := range []string{
		`{"version":2,"next_producer_epoch":18446744073709551615}`,
		`{"version":1,"next_producer_epoch":1}`,
		`{"version":2,"next_producer_epoch":1,"transaction":{"id":"a","epoch":0}}`,
		`{"version":2,"transaction":{"id":"a","epoch":-1}}`,
	} {
		_, _, err := decodeJournalRecord([]byte(payload))
		require.Error(t, err)
	}
}

func TestJournalTruncatedSoleWatermarkFailsWithoutTruncating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	require.NoError(t, j.Append(testJournalSnapshot("retired", 1, StateCommitted)))
	require.NoError(t, j.Rewrite(nil))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, length := range []int{1, 4, len(data) - 1} {
		require.NoError(t, os.WriteFile(path, data[:length], 0o600))
		_, err = j.Load()
		require.ErrorContains(t, err, "cannot safely recover")
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, data[:length], after)
	}
}
