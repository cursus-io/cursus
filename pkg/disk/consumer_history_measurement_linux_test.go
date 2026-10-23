//go:build linux

package disk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This is a local-storage fixture, NOT broker startup or distributed failover.
// OS caches are not dropped. Initial construction/appends and closing old
// handlers are excluded; reopening handlers IS included in reopen_full.
func TestConsumerHistoryDiskMeasurement(t *testing.T) {
	if os.Getenv("CURSUS_HISTORY_MEASURE") != "1" {
		t.Skip("set CURSUS_HISTORY_MEASURE=1 for isolated history reduction measurements")
	}
	for _, history := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			r := openConsumerHistoryDisks(t, cfg, history, true)
			c := historyDiskCoordinator(t, cfg, r)
			modes := []string{"full", "certify_rewrite", "reopen_full"}
			if os.Getenv("CURSUS_HISTORY_EVICT_FILES") == "1" {
				modes = []string{"reopen_evicted_baseline", "certify_rewrite", "reopen_evicted_compacted"}
			}
			for _, mode := range modes {
				if strings.HasPrefix(mode, "reopen") {
					closeConsumerHistoryDisks(t, r)
				}
				if strings.HasPrefix(mode, "reopen_evicted") {
					historyEvictOwnedFiles(t, cfg.LogDir)
				}
				r.records, r.bytes, r.syncs = 0, 0, 0
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				var peak atomic.Uint64
				peak.Store(before.HeapAlloc)
				stop, done := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(done)
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							var m runtime.MemStats
							runtime.ReadMemStats(&m)
							if m.HeapAlloc > peak.Load() {
								peak.Store(m.HeapAlloc)
							}
						}
					}
				}()
				var rewriteFsyncs, removed int
				var tempPeak, logicalRewriteRead, logicalRewriteWrite int64
				stage := func(name string) error {
					switch name {
					case "log_synced", "index_synced", "marker_synced", "generation_invalidated", "marker_published", "directory_synced":
						rewriteFsyncs++
					}
					if name == "marker_synced" {
						var temp int64
						err := filepath.WalkDir(cfg.LogDir, func(path string, entry os.DirEntry, err error) error {
							if err != nil {
								return err
							}
							if !entry.IsDir() && strings.HasSuffix(path, ".compacting") {
								info, err := entry.Info()
								if err != nil {
									return err
								}
								temp += info.Size()
							}
							return nil
						})
						if err != nil {
							return err
						}
						if temp > tempPeak {
							tempPeak = temp
						}
						logicalRewriteWrite += temp + 32 // source generation file
					}
					return nil
				}
				ioBefore, cpuBefore := historyMeasurementIO(t), historyMeasurementCPU(t)
				start := time.Now()
				switch mode {
				case "full":
					assertHistoryDiskRecovery(t, cfg, r, history)
				case "certify_rewrite":
					for p := range r.handlers {
						n, b, _, err := certifyAndRewriteHistorySegment(t, c, r, p, stage)
						require.NoError(t, err)
						removed += n
						logicalRewriteRead += b
					}
				case "reopen_full", "reopen_evicted_baseline", "reopen_evicted_compacted":
					r = openConsumerHistoryDisks(t, cfg, history, false)
					assertHistoryDiskRecovery(t, cfg, r, history)
				}
				elapsed := time.Since(start)
				cpu := historyMeasurementCPU(t) - cpuBefore
				ioAfter := historyMeasurementIO(t)
				close(stop)
				<-done
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > peak.Load() {
					peak.Store(after.HeapAlloc)
				}
				var retained int64
				require.NoError(t, filepath.WalkDir(cfg.LogDir, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !entry.IsDir() && strings.HasSuffix(path, ".log") {
						info, err := entry.Info()
						if err != nil {
							return err
						}
						retained += info.Size()
					}
					return nil
				}))
				result := map[string]any{"scope": "local_storage_os_cache_not_dropped", "history_per_group": history, "groups": 4, "partitions": 4, "mode": mode, "wall_ms": float64(elapsed) / float64(time.Millisecond), "cpu_ms": cpu * 1000, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "sampled_peak_heap_bytes": peak.Load(), "retained_log_bytes": retained, "source_records": r.records, "source_key_payload_bytes": r.bytes, "rewrite_read_frame_bytes": logicalRewriteRead, "rewrite_write_file_bytes": logicalRewriteWrite, "removed_records": removed, "rewrite_temp_peak_bytes": tempPeak, "instrumented_source_syncs": r.syncs, "instrumented_rewrite_syncs": rewriteFsyncs, "transfer_bytes": 0, "physical_read_bytes": ioAfter["read_bytes"] - ioBefore["read_bytes"], "physical_write_bytes": ioAfter["write_bytes"] - ioBefore["write_bytes"]}
				if strings.HasPrefix(mode, "reopen_evicted") {
					result["scope"] = "local_storage_log_index_eviction_requested"
				}
				encoded, err := json.Marshal(result)
				require.NoError(t, err)
				t.Log("HISTORY_MEASUREMENT " + string(encoded))
			}
			closeConsumerHistoryDisks(t, r)
		})
	}
}

// Evict only this test's synced log/index pages, never the host-wide page cache.
// FADV_DONTNEED is advisory: physical_read_bytes is reported separately and a
// zero result must not be presented as a measured cold device read.
func historyEvictOwnedFiles(t *testing.T, root string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || (!strings.HasSuffix(path, ".log") && !strings.HasSuffix(path, ".index")) {
			return err
		}
		file, err := os.Open(path) // #nosec G304 -- only this test's private temporary directory.
		if err != nil {
			return err
		}
		err = file.Sync()
		if err == nil {
			// #nosec G115 -- Linux file descriptors originate as nonnegative int values.
			err = unix.Fadvise(int(file.Fd()), 0, 0, unix.FADV_DONTNEED)
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		return closeErr
	}))
}

func historyMeasurementCPU(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func historyMeasurementIO(t *testing.T) map[string]uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/io")
	require.NoError(t, err)
	out := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			out[strings.TrimSuffix(fields[0], ":")], err = strconv.ParseUint(fields[1], 10, 64)
			require.NoError(t, err)
		}
	}
	return out
}
