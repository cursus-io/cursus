package transaction

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The controller's pending topic manifest fences access throughout Rewrite.
// Exercise its sub-phases separately, then repeat the same terminal-state prune
// that startup recovery performs before publishing the topic epoch marker.
func TestTruncateJournalProcessCrash(t *testing.T) {
	if path := os.Getenv("CURSUS_TRUNCATE_JOURNAL_PATH"); path != "" {
		journal, err := OpenJournal(path)
		require.NoError(t, err)
		snapshot := &Snapshot{ID: "tx", Producer: "p", Epoch: 5, Revision: 1, State: StateCommitted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), Messages: []MessageOperation{{Topic: "orders"}, {Topic: "audit"}}}
		require.NoError(t, journal.Append(snapshot))
		snapshot.Messages = []MessageOperation{{Topic: "audit"}}
		journal.rewritePhase = func(phase string) {
			if phase == os.Getenv("CURSUS_TRUNCATE_JOURNAL_PHASE") {
				os.Exit(86)
			}
		}
		require.NoError(t, journal.Rewrite(map[string]*Snapshot{"tx": snapshot}))
		t.Fatal("crash phase not reached")
	}
	for _, phase := range []string{"journal_synced", "journal_renamed", "journal_directory_synced", "journal_manifest_written", "journal_manifest_synced", "journal_manifest_directory_synced"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "transactions.journal")
			cmd := exec.Command(os.Args[0], "-test.run=^TestTruncateJournalProcessCrash$")
			cmd.Env = append(os.Environ(), "CURSUS_TRUNCATE_JOURNAL_PATH="+path, "CURSUS_TRUNCATE_JOURNAL_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, string(out))
			require.Equal(t, 86, exited.ExitCode(), string(out))
			info, err := os.Stat(path)
			require.NoError(t, err)
			journal, err := OpenJournal(path)
			require.NoError(t, err)
			state, err := journal.Load()
			require.NoError(t, err)
			require.GreaterOrEqual(t, journal.NextProducerEpoch(), uint64(6))
			if phase != "journal_synced" {
				require.Equal(t, []MessageOperation{{Topic: "audit"}}, state["tx"].Messages)
			}
			state["tx"].Messages = []MessageOperation{{Topic: "audit"}}
			require.NoError(t, journal.Rewrite(state))
			state, err = journal.Load()
			require.NoError(t, err)
			require.Equal(t, []MessageOperation{{Topic: "audit"}}, state["tx"].Messages)
			t.Logf("journal recovery input_bytes=%d transactions=%d records=%d; repeat prune preserves producer epoch floor", info.Size(), len(state), journal.records)
		})
	}
}
