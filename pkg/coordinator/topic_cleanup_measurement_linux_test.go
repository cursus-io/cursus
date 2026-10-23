//go:build linux

package coordinator

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCleanupLargeRecordMeasuredWork(t *testing.T) {
	if os.Getenv("RUN_CLEANUP_RECORD_MEASUREMENT") != "1" {
		t.Skip("opt-in isolated process measurement")
	}
	for _, topics := range []int{0, 32, 128} {
		t.Run(fmt.Sprint(topics), func(t *testing.T) {
			var messages []types.Message
			for i := 0; i < 257; i++ {
				if topics > 0 && i == 128 {
					messages = append(messages, cleanupMessage(t, largeCleanupRecord(topics, 1024)))
				}
				messages = append(messages, cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: fmt.Sprintf("history-%d", i), Topic: "retired", Epoch: 2}))
			}
			messages = append(messages, cleanupMessage(t, cleanupDeletion()))
			view := newCleanupDiskView(t, messages...)
			scan := &topicCleanupScan{targets: map[string]*cleanupTarget{"workers": {epoch: 1}}, status: TopicCleanupPending{Topic: "orders"}}
			c, _ := newCleanupFollower(t)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			var cpuBefore, cpuAfter syscall.Rusage
			require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore))
			ioBefore, err := os.ReadFile("/proc/self/io")
			require.NoError(t, err)
			done := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			var peak uint64
			var latencies []time.Duration
			go func() {
				defer wg.Done()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						var stat runtime.MemStats
						runtime.ReadMemStats(&stat)
						if stat.HeapAlloc > peak {
							peak = stat.HeapAlloc
						}
						start := time.Now()
						_ = c.GetGroup("workers")
						latencies = append(latencies, time.Since(start))
					}
				}
			}()
			started := time.Now()
			turns := 0
			for turns < 100 {
				turns++
				err = scan.readContext(context.Background(), view, time.Now)
				if err == nil {
					break
				}
				require.ErrorIs(t, err, errCleanupBudget)
			}
			elapsed := time.Since(started)
			close(done)
			wg.Wait()
			require.NoError(t, err)
			runtime.ReadMemStats(&after)
			require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter))
			ioAfter, err := os.ReadFile("/proc/self/io")
			require.NoError(t, err)
			require.Equal(t, len(messages), view.decoded, "no repeated payload decoding")
			require.Equal(t, uint64(2), scan.targets["workers"].evidence.epoch)
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			var p99 time.Duration
			if len(latencies) > 0 {
				p99 = latencies[(len(latencies)-1)*99/100]
			}
			cpu := func(r syscall.Rusage) time.Duration {
				return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
			}
			t.Logf("topics=%d turns=%d records=%d decoded=%d payload_frame_bytes=%d logical_read_bytes=%d elapsed=%s cpu=%s allocated_bytes=%d sampled_peak_heap=%d baseline_heap=%d get_group_samples=%d get_group_p99=%s", topics, turns, len(messages), view.decoded, scan.status.Bytes, scan.status.ReadBytes, elapsed, cpu(cpuAfter)-cpu(cpuBefore), after.TotalAlloc-before.TotalAlloc, peak, before.HeapAlloc, len(latencies), p99)
			t.Logf("proc_io_before=%s proc_io_after=%s", strings.ReplaceAll(string(ioBefore), "\n", ", "), strings.ReplaceAll(string(ioAfter), "\n", ", "))
		})
	}
}

func TestCleanupLargeRecordMeasuredCancellation(t *testing.T) {
	if os.Getenv("RUN_CLEANUP_RECORD_MEASUREMENT") != "1" {
		t.Skip("opt-in isolated process measurement")
	}
	view := newCleanupDiskView(t, cleanupMessage(t, largeCleanupRecord(128, 1024)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan time.Time, 1)
	view.afterRead = func() {
		if view.calls == 2 {
			go func() {
				time.Sleep(10 * time.Millisecond)
				at := time.Now()
				cancel()
				cancelled <- at
			}()
		}
	}
	scan := &topicCleanupScan{}
	err := scan.readContext(ctx, view, time.Now)
	returned := time.Now()
	at := <-cancelled
	if returned.Before(at) {
		require.NoError(t, err)
		t.Log("scan completed before the delayed cancellation; no in-flight response sample")
		return
	}
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, scan.next[0])
	require.Empty(t, cleanupReplaySlot)
	t.Logf("cancel_scheduled_after_disk_read=10ms cancellation_response=%s serialized_bytes=%d logical_read_bytes=%d disk_decodes=%d", returned.Sub(at), scan.status.Bytes, scan.status.ReadBytes, view.decoded)
}
