//go:build linux

package topic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// This fixture also compiles unchanged at the incident commit. The scheduler
// wait is injected/accounted, never slept for 30+ minutes. Wall/CPU/I/O work is
// measured separately from that simulated wait and from fixture construction.
type cleanupMeasurementReader struct {
	*topic.TopicManager
	records, bytes, reads uint64
}
type cleanupMeasurementView struct {
	coordinator.ConsumerCleanupView
	r *cleanupMeasurementReader
}

func (r *cleanupMeasurementReader) WithConsumerCleanupView(fn func(coordinator.ConsumerCleanupView) error) error {
	return r.TopicManager.WithConsumerCleanupView(func(v coordinator.ConsumerCleanupView) error { return fn(&cleanupMeasurementView{v, r}) })
}
func (r *cleanupMeasurementReader) WithConsumerCleanupViewContext(ctx context.Context, fn func(coordinator.ConsumerCleanupView) error) error {
	if reader, ok := any(r.TopicManager).(interface {
		WithConsumerCleanupViewContext(context.Context, func(coordinator.ConsumerCleanupView) error) error
	}); ok {
		return reader.WithConsumerCleanupViewContext(ctx, func(v coordinator.ConsumerCleanupView) error { return fn(&cleanupMeasurementView{v, r}) })
	}
	return r.WithConsumerCleanupView(fn)
}
func (v *cleanupMeasurementView) Read(p int, o uint64, c types.ReplayCursor, n, b int) (coordinator.ConsumerCleanupPage, error) {
	page, err := v.ConsumerCleanupView.Read(p, o, c, n, b)
	v.r.reads++
	v.r.records += uint64(page.Records)
	v.r.bytes += uint64(page.Bytes)
	return page, err
}
func cleanupCPU() float64 {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func TestCleanupHistoryMeasurement(t *testing.T) {
	mode := os.Getenv("CURSUS_CLEANUP_MEASURE")
	if mode == "" {
		t.Skip("opt-in disk/CPU measurement; set CURSUS_CLEANUP_MEASURE=baseline or continuation")
	}
	const count = 56000
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { tm.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, tm.CreateTopic(config.ConsumerOffsetsTopicName, 4, false, false))
	require.NoError(t, tm.CreateTopic("requests", 1, false, false))
	for _, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		require.NoError(t, p.ApplyReplicaHWM(0))
	}
	reader := &cleanupMeasurementReader{TopicManager: tm}
	groups, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, reader)
	require.NoError(t, err)
	t.Cleanup(groups.Stop)
	for partition, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		for start := 0; start < count/4; start += 250 {
			batch := make([]types.Message, 0, 250)
			for i := start; i < min(start+250, count/4); i++ {
				record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "unrelated", Topic: "audit", Epoch: 1, Revision: uint64(i + 1), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}, Timestamp: time.Unix(1, 0)}
				if partition == 3 && i == count/4-1 {
					record = coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone, Group: "workers", Topic: "orders", Epoch: 2}
				}
				payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
				require.NoError(t, err)
				metadata := ""
				if i%512 < 20 {
					metadata = strings.Repeat("x", 60<<10)
				}
				batch = append(batch, types.Message{Offset: uint64(i), Key: key, Payload: string(payload), Metadata: metadata})
			}
			require.NoError(t, p.ReplicaAppend(batch))
		}
		require.NoError(t, p.ApplyReplicaHWM(count/4))
	}
	require.NoError(t, groups.ImportState(map[string]*coordinator.GroupStateSnapshot{"workers": {TopicName: "orders", Partitions: []int{0}, RegistrationEpoch: 1, LastActivity: time.Now(), Members: map[string][]int{"stale": {0}}, Offsets: map[string]map[int]uint64{"orders": {0: 17}}}}))
	groups.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return fmt.Errorf("unexpected follower write") })
	// A concurrent request path uses a different partition and the coordinator's
	// nonblocking Raft-Apply cleanup check. Report its actual latency distribution.
	var stop atomic.Bool
	done := make(chan []time.Duration, 1)
	go func() {
		var samples []time.Duration
		for !stop.Load() {
			started := time.Now()
			_ = tm.GetTopic("requests").Partitions[0].NextOffset()
			_, _ = groups.CheckTopicGroupCleanup("orders")
			samples = append(samples, time.Since(started))
			time.Sleep(time.Millisecond)
		}
		done <- samples
	}()
	runtime.GC()
	var initial, final runtime.MemStats
	runtime.ReadMemStats(&initial)
	peak := initial.HeapAlloc
	cpu := cleanupCPU()
	started := time.Now()
	batches := 0
	var virtualWait time.Duration
	for groups.GetGroup("workers") != nil && batches < 5000 {
		batches++
		_, err := groups.ReconcileTopicGroups("orders")
		if err != nil {
			if mode == "baseline" {
				virtualWait += 5 * time.Second
			} else {
				data, _ := json.Marshal(err)
				var state struct {
					State string `json:"state"`
				}
				_ = json.Unmarshal(data, &state)
				if state.State == "continue" {
					virtualWait += time.Millisecond
				} else {
					virtualWait += 5 * time.Second
				}
			}
		}
		runtime.ReadMemStats(&final)
		peak = max(peak, final.HeapAlloc)
		runtime.Gosched()
	}
	elapsed := time.Since(started)
	cpu = cleanupCPU() - cpu
	stop.Store(true)
	samples := <-done
	require.Nil(t, groups.GetGroup("workers"))
	require.Equal(t, uint64(count), reader.records)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var p99, maxLatency time.Duration
	if len(samples) > 0 {
		p99 = samples[(len(samples)-1)*99/100]
		maxLatency = samples[len(samples)-1]
	}
	result := map[string]any{"mode": mode, "history": count, "batches": batches, "reads": reader.reads, "physical_page_bytes": reader.bytes, "active_wall_seconds": elapsed.Seconds(), "process_cpu_seconds": cpu, "allocated_bytes": final.TotalAlloc - initial.TotalAlloc, "peak_sampled_heap_bytes": peak, "records_per_active_second": float64(count) / elapsed.Seconds(), "simulated_wait_seconds": virtualWait.Seconds(), "other_operations": len(samples), "other_p99_ns": p99.Nanoseconds(), "other_max_ns": maxLatency.Nanoseconds(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	data, _ := json.Marshal(result)
	t.Logf("CLEANUP_MEASUREMENT %s", data)
}

func (v *cleanupMeasurementView) PrepareCleanupCheckpoint() error {
	if view, ok := v.ConsumerCleanupView.(interface{ PrepareCleanupCheckpoint() error }); ok {
		return view.PrepareCleanupCheckpoint()
	}
	return nil
}
