//go:build linux

package fsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

// TestCheckpointWorkload is an opt-in, fixed-rate storage/FSM experiment. It
// deliberately excludes Raft transport and client round trips; its commit
// latency is local FSM commit application plus the unchanged disk barrier.
// Copy this file unchanged to the baseline checkout to compare implementations.
func TestCheckpointWorkload(t *testing.T) {
	output := os.Getenv("CURSUS_CHECKPOINT_RESULT")
	if output == "" {
		t.Skip("set CURSUS_CHECKPOINT_RESULT to run the I/O experiment")
	}
	util.SetLevel(util.LogLevelError)
	t.Cleanup(func() { util.SetLevel(util.LogLevelDebug) })
	seconds := 30
	if value := os.Getenv("CURSUS_CHECKPOINT_SECONDS"); value != "" {
		var err error
		seconds, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	require.Positive(t, seconds)
	load := os.Getenv("CURSUS_CHECKPOINT_LOAD") == "1"
	const partitions = 8
	const rate = 2
	ids := []string{"node-0", "node-1", "node-2"}
	states := make([]*BrokerFSM, 3)
	local := make([][]*topic.Partition, 3)
	for replica := range states {
		cfg := config.DefaultConfig()
		cfg.LogDir = filepath.Join(t.TempDir(), "data")
		cfg.DiskMinFreeBytes = 1
		cfg.DiskMinFreePercent = 0.1
		dm := disk.NewDiskManager(cfg)
		t.Cleanup(dm.CloseAllHandlers)
		tm := topic.NewTopicManager(cfg, dm, nil)
		require.NoError(t, tm.CreateTopic("checkpoint-load", partitions, false, true))
		state := NewBrokerFSM(tm, nil)
		states[replica] = state
		for _, id := range ids {
			registerActiveBroker(t, state, id)
		}
		definition := tm.GetTopic("checkpoint-load").Definition()
		state.topicState["checkpoint-load"] = &definition
		for id := 0; id < partitions; id++ {
			p, err := tm.GetTopic("checkpoint-load").GetPartition(id)
			require.NoError(t, err)
			t.Cleanup(p.Close)
			local[replica] = append(local[replica], p)
			require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 1, SeqNum: 1, Payload: strings.Repeat("x", 1024)}))
			p.FlushDisk()
			state.partitionMetadata[fmt.Sprintf("checkpoint-load-%d", id)] = &PartitionMetadata{Leader: ids[0], LeaderEpoch: 1, LifecycleEpoch: definition.LifecycleEpoch, CommittedHWM: 1, CommittedHWMKnown: true, PartitionCount: partitions, Replicas: ids, ISR: ids}
		}
	}
	// Let initial coalesced checkpoint notifications and flush timers settle.
	time.Sleep(time.Second)
	beforeIO := checkpointProcIO(t)
	beforeCgroup, cgroupErr := os.ReadFile("/sys/fs/cgroup/io.stat")
	beforeStat, err := os.ReadFile("/proc/stat")
	require.NoError(t, err)
	var beforeUsage, afterUsage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &beforeUsage))
	start := time.Now()
	appendUS, commitUS := []int64{}, []int64{}
	seq := make([]uint64, partitions)
	for i := range seq {
		seq[i] = 1
	}
	tickCount := 0
	for tick := 0; tick < seconds*rate; tick++ {
		time.Sleep(time.Until(start.Add(time.Duration(tick) * time.Second / rate)))
		if tick%rate == 0 {
			for replica, state := range states {
				require.Empty(t, state.BuildReplicaCatchupRequests(ids[replica]))
			}
			tickCount++
		}
		if !load {
			continue
		}
		id := tick % partitions
		seq[id]++
		msg := types.Message{ProducerID: "p", Epoch: 1, SeqNum: seq[id], Payload: strings.Repeat("x", 1024), LeaderEpoch: 1}
		begin := time.Now()
		for replica := range states {
			require.NoError(t, local[replica][id].EnqueueBatchLeaderWithMode([]types.Message{msg}, true))
		}
		appendUS = append(appendUS, time.Since(begin).Microseconds())
		begin = time.Now()
		for replica, state := range states {
			metadata := state.partitionMetadata[fmt.Sprintf("checkpoint-load-%d", id)]
			data, err := json.Marshal(partitionCommitCommand{Topic: "checkpoint-load", Partition: id, Leader: ids[0], LeaderEpoch: 1, HWM: seq[id], LifecycleEpoch: metadata.LifecycleEpoch})
			require.NoError(t, err)
			require.Nil(t, state.applyPartitionCommitCommand(string(data)))
			require.Equal(t, seq[id], local[replica][id].GetHWM())
		}
		commitUS = append(commitUS, time.Since(begin).Microseconds())
	}
	time.Sleep(time.Until(start.Add(time.Duration(seconds) * time.Second)))
	end := time.Now()
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &afterUsage))
	afterIO := checkpointProcIO(t)
	for key, value := range beforeIO {
		afterIO[key] -= value
	}
	afterStat, err := os.ReadFile("/proc/stat")
	require.NoError(t, err)
	afterCgroup, _ := os.ReadFile("/sys/fs/cgroup/io.stat")
	result := map[string]any{"start_utc": start.UTC(), "end_utc": end.UTC(), "start_unix": float64(start.UnixNano()) / 1e9, "end_unix": float64(end.UnixNano()) / 1e9, "seconds": end.Sub(start).Seconds(), "replicas": 3, "partitions_per_replica": partitions, "payload_bytes": 1024, "load": load, "messages": len(appendUS), "catchup_ticks": tickCount, "user_seconds": float64(afterUsage.Utime.Nano()-beforeUsage.Utime.Nano()) / 1e9, "system_seconds": float64(afterUsage.Stime.Nano()-beforeUsage.Stime.Nano()) / 1e9, "io": afterIO, "append_us": checkpointPercentiles(appendUS), "local_commit_us": checkpointPercentiles(commitUS), "host_cpu_before": strings.Split(string(beforeStat), "\n")[0], "host_cpu_after": strings.Split(string(afterStat), "\n")[0]}
	if cgroupErr != nil {
		result["cgroup_io_unavailable"] = cgroupErr.Error()
	}
	result["cgroup_io_before"] = string(beforeCgroup)
	result["cgroup_io_after"] = string(afterCgroup)
	data, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, data, 0o600))
}

func checkpointProcIO(t *testing.T) map[string]int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/io")
	require.NoError(t, err)
	result := map[string]int64{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		value, err := strconv.ParseInt(fields[1], 10, 64)
		require.NoError(t, err)
		result[strings.TrimSuffix(fields[0], ":")] = value
	}
	return result
}
func checkpointPercentiles(values []int64) map[string]int64 {
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return map[string]int64{"p50": values[len(values)*50/100], "p95": values[len(values)*95/100], "p99": values[len(values)*99/100], "max": values[len(values)-1]}
}
