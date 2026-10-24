package coordinator

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// This is a deterministic process-crash correctness test of the replay layer,
// not a whole-broker timing result. A child exits after consuming a source page,
// before projection publication; the next process must repeat that work safely.
func TestConsumerReplayRepeatedProcessCrashDuringScan(t *testing.T) {
	if stop := os.Getenv("CURSUS_REPLAY_STOP_AFTER"); stop != "" {
		limit, err := strconv.Atoi(stop)
		require.NoError(t, err)
		c, r := newIndexFixture(t, 3000)
		c.cfg.LogDir = os.Getenv("CURSUS_REPLAY_RESTART_DIR")
		consumed := 0
		r.read = func(partition int, next uint64, max int) ([]types.Message, error) {
			var page []types.Message
			for _, m := range r.messages[partition] {
				if m.Offset >= next && len(page) < max {
					page = append(page, m)
				}
			}
			consumed += len(page)
			if consumed >= limit {
				fmt.Printf("consumed_source_records=%d\n", consumed)
				os.Exit(77)
			}
			return page, nil
		}
		_, err = c.PrepareFullConsumerMetadataReplay(r)
		t.Fatalf("replay crash point not reached: %v", err)
	}
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			c, r := newIndexFixture(t, 3000)
			path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
			var original []byte
			if cached {
				r.end = map[int]uint64{0: 100}
				// The source itself must enforce its committed cut.
				all := r.messages[0]
				r.messages[0] = all[:100]
				_, err := c.PrepareFullConsumerMetadataReplay(r)
				require.NoError(t, err)
				original, err = os.ReadFile(path)
				require.NoError(t, err)
				r.messages[0], r.end = all, nil
			}
			for _, stop := range []string{"1024", "2048", "1024"} {
				command := exec.Command(os.Args[0], "-test.run=^TestConsumerReplayRepeatedProcessCrashDuringScan$") // #nosec G204 -- this test binary, no external program.
				command.Env = append(os.Environ(), "CURSUS_REPLAY_STOP_AFTER="+stop, "CURSUS_REPLAY_RESTART_DIR="+c.cfg.LogDir)
				output, err := command.CombinedOutput()
				var exited *exec.ExitError
				require.ErrorAs(t, err, &exited, string(output))
				require.Equal(t, 77, exited.ExitCode(), string(output))
				require.Contains(t, string(output), "consumed_source_records=")
				t.Logf("interrupted scan cached=%v threshold=%s: %s", cached, stop, output)
				if cached {
					current, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, original, current, "incomplete suffix cannot certify a new cache")
				} else {
					require.NoFileExists(t, path)
				}
			}
			replay, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.Equal(t, cached, replay.status.IndexHit)
			require.Equal(t, uint64(3000), replay.scratch.groups["g"].Offsets["events"][0])
			again, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.True(t, again.status.IndexHit)
			require.Equal(t, replay.scratch.ExportState(), again.scratch.ExportState())
		})
	}
}
