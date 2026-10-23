//go:build linux

package controller

import (
	"context"
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

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// This is an opt-in real-disk, local controller recovery measurement. It is NOT
// whole broker startup, a multi-broker failover or a cold OS page-cache test.
// Fixture construction, initial appends and topology setup are outside timing.
func TestConsumerReplayIndexDiskMeasurement(t *testing.T) {
	if os.Getenv("CURSUS_INDEX_MEASURE") != "1" {
		t.Skip("set CURSUS_INDEX_MEASURE=1 for local disk recovery measurements")
	}
	for _, history := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			cfg.DiskMinFreePercent = 0 // isolated fixture; preserve absolute free-space guard
			dm := disk.NewDiskManager(cfg)
			t.Cleanup(dm.CloseAllHandlers)
			tm := topic.NewTopicManager(cfg, dm, nil)
			t.Cleanup(tm.Stop)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			state := fsm.NewBrokerFSM(tm, c)
			require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"local","addr":"127.0.0.1:7000","status":"active","lifecycle_protocol":7}`)}))
			definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
			definition.Partitions = 4
			definition.ReplicationFactor = 1
			definition.Policy = topic.ConsumerMetadataPolicy()
			definition.InstanceID = "index-measurement"
			payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "local"})
			require.NoError(t, err)
			require.Nil(t, state.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
			h := newCoordinatorRoutingHandler("local", state, c)
			h.TopicManager = tm
			h.Config = cfg
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			for partition, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
				p.SetHWM(0)
				group := fmt.Sprintf("g%d", partition)
				record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordRegistration, Group: group, Topic: "orders", PartitionCount: 1, Epoch: 1}
				messages := []types.Message{consumerMetadataMessage(t, record, 0)}
				for i := 1; i <= history; i++ {
					record = coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: group, Topic: "orders", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
					messages = append(messages, consumerMetadataMessage(t, record, uint64(i)))
				}
				require.NoError(t, p.ReplicaAppend(messages))
				require.NoError(t, p.ApplyReplicaHWM(uint64(history+1)))
				p.FlushDisk()
				metadata := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, partition))
				metadata.CommittedHWM = uint64(history + 1)
				applyProjectionMetadata(t, state, partition, metadata)
			}
			for _, mode := range []string{"full", "build", "valid", "missing", "corrupt"} {
				path := filepath.Join(cfg.LogDir, ".consumer-replay-index.json")
				cfg.ConsumerRecoveryIndexEnabled = mode != "full"
				if mode == "missing" {
					require.NoError(t, os.Remove(path))
				}
				if mode == "corrupt" {
					require.NoError(t, os.WriteFile(path, []byte("torn"), 0600))
				}
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
				ioBefore := replayMeasurementIO()
				cpuBefore := replayMeasurementCPU()
				start := time.Now()
				err = h.recoverDistributedConsumerMetadata(context.Background(), true)
				elapsed := time.Since(start)
				cpu := replayMeasurementCPU() - cpuBefore
				ioAfter := replayMeasurementIO()
				close(stop)
				<-done
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > peak.Load() {
					peak.Store(after.HeapAlloc)
				}
				require.NoError(t, err)
				for group := 0; group < 4; group++ {
					value, ok := c.GetOffset(fmt.Sprintf("g%d", group), "orders", 0)
					require.True(t, ok)
					require.Equal(t, uint64(history), value)
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
				result := map[string]any{"scope": "local_controller_recovery_os_cache_not_dropped", "mode": mode, "history_per_group": history, "groups": 4, "partitions": 4, "wall_ms": float64(elapsed) / float64(time.Millisecond), "cpu_ms": cpu * 1000, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "sampled_peak_heap_bytes": peak.Load(), "retained_log_bytes": retained, "recovery": c.RecoverySnapshot(), "transfer_bytes": 0, "physical_read_bytes": ioAfter["read_bytes"] - ioBefore["read_bytes"], "physical_write_bytes": ioAfter["write_bytes"] - ioBefore["write_bytes"], "os_rchar": ioAfter["rchar"] - ioBefore["rchar"], "os_wchar": ioAfter["wchar"] - ioBefore["wchar"]}
				encoded, err := json.Marshal(result)
				require.NoError(t, err)
				t.Log("INDEX_MEASUREMENT " + string(encoded))
			}
		})
	}
}

func replayMeasurementCPU() float64 {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}
func replayMeasurementIO() map[string]uint64 {
	data, _ := os.ReadFile("/proc/self/io")
	out := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			out[strings.TrimSuffix(fields[0], ":")], _ = strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return out
}
