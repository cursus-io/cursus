package e2e_cluster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// This is an opt-in whole-process experiment, not a microbenchmark. Baseline
// disables history maintenance in the same binary. Four groups and all topic
// partitions stay fixed. Initial seeding and maintenance have separate rows.
func TestConsumerHistoryWholeBrokerLadder(t *testing.T) {
	if os.Getenv("RUN_E2E_CONSUMER_HISTORY_MEASURE") != "1" {
		t.Skip("opt-in resource-intensive isolated history experiment")
	}
	require.NotZero(t, cleanupPortOffset())
	output := os.Getenv("CURSUS_HISTORY_MEASURE_OUTPUT")
	require.NotEmpty(t, output, "an explicit output JSONL path is required")
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	emit := func(row map[string]any) {
		require.NoError(t, json.NewEncoder(file).Encode(row))
		require.NoError(t, file.Sync())
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	diff, err := exec.Command("git", "diff", "--binary", "HEAD").Output()
	require.NoError(t, err)
	emit(map[string]any{"phase": "experiment", "source_head": strings.TrimSpace(string(head)), "source_clean": len(diff) == 0,
		"source_diff_sha256": fmt.Sprintf("%x", sha256.Sum256(diff)), "driver_go": runtime.Version(),
		"broker_cpus": 2, "broker_memory_bytes": 1 << 30, "trace": "strace -f --seccomp-bpf -e trace=fsync,fdatasync",
		"logical_groups": 4, "metadata_partitions": 4, "replication_factor": 3})
	levels := []int{100, 1000, 10000}
	if value := os.Getenv("CURSUS_HISTORY_MEASURE_LEVELS"); value != "" {
		levels = nil
		for _, field := range strings.Split(value, ",") {
			n, err := strconv.Atoi(field)
			require.NoError(t, err)
			require.Positive(t, n)
			levels = append(levels, n)
		}
	}
	for _, history := range levels {
		t.Run(strconv.Itoa(history), func(t *testing.T) {
			t.Setenv("CURSUS_HISTORY_MEASURE_COMPACTION", "false")
			files := []string{composeFile, "docker-compose.consumer-history.yml", "docker-compose.history-measure.yml"}
			setup := time.Now()
			ctx := givenCleanupCluster(t, files...).WithClusterSize(3).WithTopic("history-measure").WithPartitions(1).WithAcks("all")
			defer ctx.Cleanup()
			actions := ctx.WhenCluster().StartCluster()
			project, isolated := isolateCleanupCompose(t, "cursus-cleanup-e2e", files)
			compose := func(args ...string) {
				data, err := e2e.RunCompose(composeCommandArgs(project, isolated, args...)...).CombinedOutput()
				require.NoError(t, err, string(data))
			}
			emit(map[string]any{"history": history, "phase": "initial_cluster_setup", "wall_seconds": time.Since(setup).Seconds()})
			sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-measure partitions=1 replication_factor=3")
			start := time.Now()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var latencies []time.Duration
			retries := 0
			for group := 0; group < 4; group++ {
				name := fmt.Sprintf("history-measure-%d", group)
				client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-measure", name)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer client.Close()
					var times []time.Duration
					attempts := 0
					for revision := 1; revision <= history; revision++ {
						began := time.Now()
						attempts += consumerHistoryCommand(t, client, fmt.Sprintf("COMMIT_OFFSET topic=history-measure partition=0 group=%s offset=%d generation=%d member=%s", name, revision, generation, member))
						times = append(times, time.Since(began))
						if revision%5 == 0 {
							consumerHistoryCommand(t, client, fmt.Sprintf("HEARTBEAT topic=history-measure group=%s generation=%d member=%s", name, generation, member))
						}
					}
					require.Equal(t, uint64(history), fetchCommittedOffset(t, client, "history-measure", name))
					consumerHistoryCommand(t, client, fmt.Sprintf("LEAVE_GROUP topic=history-measure group=%s generation=%d member=%s", name, generation, member))
					mu.Lock()
					latencies = append(latencies, times...)
					retries += attempts
					mu.Unlock()
				}()
			}
			wg.Wait()
			slices.Sort(latencies)
			require.Len(t, latencies, 4*history)
			emit(map[string]any{"history": history, "phase": "seed", "wall_seconds": time.Since(start).Seconds(), "acknowledged_commits": len(latencies), "commit_p50_ms": float64(latencies[len(latencies)/2]) / float64(time.Millisecond), "commit_p95_ms": float64(latencies[len(latencies)*95/100]) / float64(time.Millisecond), "commit_max_ms": float64(latencies[len(latencies)-1]) / float64(time.Millisecond), "availability_retries": retries, "resources": historyMeasureResources(t)})
			exact := func(addrs []string) {
				client := e2e.NewBrokerClient(addrs)
				defer client.Close()
				require.NoError(t, eventually(t, "exact offsets after whole broker recovery", 2*time.Minute, func() (bool, string, error) {
					for group := 0; group < 4; group++ {
						value, err := client.FetchCommittedOffset("history-measure", 0, fmt.Sprintf("history-measure-%d", group))
						if err != nil {
							return false, fmt.Sprint(err), nil
						}
						if value != uint64(history) {
							return false, "committed offset changed", fmt.Errorf("group=%d got=%d want=%d", group, value, history)
						}
					}
					return true, "exact committed offsets", nil
				}))
			}
			for _, variant := range []string{"baseline", "compacted"} {
				if variant == "compacted" {
					before := historyMeasureResources(t)
					t.Setenv("CURSUS_HISTORY_MEASURE_COMPACTION", "true")
					began := time.Now()
					compose("up", "-d", "--no-build", "--force-recreate")
					require.NoError(t, actions.checkAllNodesHealth())
					require.NoError(t, eventually(t, "history sweep on every replica", 5*time.Minute, func() (bool, string, error) {
						for node := 1; node <= 3; node++ {
							count, err := consumerHistoryRemoved(node)
							if err != nil {
								return false, "metrics", err
							}
							if count < float64(history*3) {
								return false, fmt.Sprintf("node=%d removed=%v", node, count), nil
							}
							// A percentage reduction alone still permits history-linear
							// retained bytes. Require a history-independent bound: four
							// 64 KiB active tails plus 64 KiB for fixed state witnesses.
							data := historyMeasureDocker(t, "exec", cleanupContainerName(node), "python3", "/app/history_measure.py", "retained-bytes")
							retained, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
							if err != nil {
								return false, "retained bytes", err
							}
							if retained > 5*65536 {
								return false, fmt.Sprintf("node=%d retained=%d exceeds fixed bound", node, retained), nil
							}
						}
						return true, "compacted", nil
					}))
					exact(ctx.GetBrokerAddrs())
					emit(map[string]any{"history": history, "variant": variant, "phase": "maintenance_including_recreate", "wall_seconds": time.Since(began).Seconds(), "before": before, "after": historyMeasureResources(t), "metrics": historyMeasureMetrics(t)})
				}
				for _, mode := range []string{"absent", "valid", "corrupt", "old"} {
					compose("stop")
					cache := "cold"
					if mode == "valid" {
						cache = "warm"
					}
					for node := 1; node <= 3; node++ {
						historyMeasureOffline(t, node, mode, cache)
					}
					began := time.Now()
					compose("start")
					require.NoError(t, actions.checkAllNodesHealth())
					waitForStableFullISRAndZeroUnderReplicated(t, ctx, "whole history restart")
					exact(ctx.GetBrokerAddrs())
					emit(map[string]any{"history": history, "variant": variant, "phase": "whole_cluster_start", "index": mode, "page_cache": cache, "wall_seconds": time.Since(began).Seconds(), "resources": historyMeasureResources(t), "metrics": historyMeasureMetrics(t)})
				}
				// Interrupt startup, then restart again. There is no readiness wait before
				// kill, but this experiment alone cannot prove the exact replay phase.
				compose("stop")
				for node := 1; node <= 3; node++ {
					historyMeasureOffline(t, node, "absent", "cold")
				}
				began := time.Now()
				compose("start")
				compose("kill", "-s", "SIGKILL")
				compose("start")
				require.NoError(t, actions.checkAllNodesHealth())
				exact(ctx.GetBrokerAddrs())
				emit(map[string]any{"history": history, "variant": variant, "phase": "interrupted_start_then_restart", "wall_seconds": time.Since(began).Seconds(), "resources_last_process_only": historyMeasureResources(t), "metrics": historyMeasureMetrics(t)})
				oldID := findGroupCoordinator(t, 1, "history-measure-0")
				oldNode := coordinatorNode(t, oldID)
				before := historyMeasureResources(t)
				beforeMetrics := historyMeasureMetrics(t)
				began = time.Now()
				historyMeasureDocker(t, "stop", cleanupContainerName(oldNode))
				survivors := survivorNodes(oldNode)
				require.NoError(t, eventually(t, "history coordinator move", clusterReadyTimeout, func() (bool, string, error) {
					for _, node := range survivors {
						id, err := findGroupCoordinatorFrom(node, "history-measure-0")
						if err != nil || id == oldID {
							return false, fmt.Sprintf("node=%d owner=%s err=%v", node, id, err), nil
						}
					}
					return true, "new owner", nil
				}))
				exact(clusterBrokerAddrsForNodes(survivors))
				emit(map[string]any{"history": history, "variant": variant, "phase": "coordinator_failover", "wall_seconds": time.Since(began).Seconds(), "stopped_node": oldNode, "before": before, "before_metrics": beforeMetrics, "after_survivors": historyMeasureResourcesFor(t, survivors), "metrics_survivors": historyMeasureMetricsFor(t, survivors)})
				historyMeasureDocker(t, "start", cleanupContainerName(oldNode))
				require.NoError(t, actions.checkAllNodesHealth())
				waitForStableFullISRAndZeroUnderReplicated(t, ctx, "history failover return")
			}
		})
		if t.Failed() {
			return
		} // Do not seed larger histories after a failed level.
	}
}

func historyMeasureDocker(t *testing.T, args ...string) []byte {
	t.Helper()
	data, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, string(data))
	return data
}
func historyMeasureOffline(t *testing.T, node int, mode, cache string) {
	t.Helper()
	name := cleanupContainerName(node)
	project := fmt.Sprintf("cursus-truncate-%d-e2e", cleanupPortOffset())
	label := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, name)))
	require.Equal(t, project, label, "offline changes are restricted to this fixture")
	running := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.State.Running}}", name)))
	require.Equal(t, "false", running, "offline changes require a stopped broker container")
	image := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.Image}}", name)))
	historyMeasureDocker(t, "run", "--rm", "--network", "none", "--pull", "never", "--volumes-from", name, "--entrypoint", "python3", image, "/app/history_measure.py", "offline", mode, cache)
}
func historyMeasureResources(t *testing.T) map[int]map[string]float64 {
	return historyMeasureResourcesFor(t, []int{1, 2, 3})
}
func historyMeasureResourcesFor(t *testing.T, nodes []int) map[int]map[string]float64 {
	out := map[int]map[string]float64{}
	for _, node := range nodes {
		data := historyMeasureDocker(t, "exec", cleanupContainerName(node), "python3", "/app/history_measure.py", "sample")
		var values map[string]float64
		require.NoError(t, json.Unmarshal(data, &values))
		out[node] = values
	}
	return out
}
func historyMeasureMetrics(t *testing.T) map[int]map[string]float64 {
	return historyMeasureMetricsFor(t, []int{1, 2, 3})
}
func historyMeasureMetricsFor(t *testing.T, nodes []int) map[int]map[string]float64 {
	out := map[int]map[string]float64{}
	for _, node := range nodes {
		client := http.Client{Timeout: 5 * time.Second}
		var response *http.Response
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			response, err = client.Get(fmt.Sprintf("http://localhost:%d/metrics", baseExporterPort+clusterPortShift()+node))
			if err == nil {
				break
			}
		}
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		values := map[string]float64{}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			key := fields[0]
			if strings.HasPrefix(key, "cursus_recovery_work_total") || strings.HasPrefix(key, "cursus_consumer_history_") || strings.HasPrefix(key, "cursus_consumer_metadata_") {
				value, err := strconv.ParseFloat(fields[1], 64)
				require.NoError(t, err)
				values[key] = value
			}
		}
		out[node] = values
	}
	return out
}
