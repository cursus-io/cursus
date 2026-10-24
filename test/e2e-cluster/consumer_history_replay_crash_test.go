package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// The normal broker ladder measures restart wall time. This separate fault
// image proves that each interrupted process actually consumed replay pages,
// and retains its counters instead of reporting only the final process.
func TestConsumerHistoryReplayCrashResources(t *testing.T) {
	if os.Getenv("RUN_E2E_CONSUMER_HISTORY_REPLAY_CRASH") != "1" {
		t.Skip("opt-in process-crash accounting experiment")
	}
	require.NotZero(t, cleanupPortOffset())
	path := os.Getenv("CURSUS_HISTORY_REPLAY_CRASH_OUTPUT")
	require.NotEmpty(t, path)
	output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer func() { require.NoError(t, output.Close()) }()
	emit := func(row map[string]any) {
		require.NoError(t, json.NewEncoder(output).Encode(row))
		require.NoError(t, output.Sync())
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	t.Setenv("CURSUS_HISTORY_MEASURE_COMPACTION", "false")
	t.Setenv("CURSUS_HISTORY_TRACE_SYNC", "true")
	const topic, revisions = "history-replay-crash", 1000
	setup := time.Now()
	ctx := givenCleanupCluster(t, composeFile, "docker-compose.consumer-history.yml", "docker-compose.history-measure.yml", "docker-compose.history-replay-crash.yml").WithClusterSize(3).WithTopic(topic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+topic+" partitions=1 replication_factor=3")
	emit(map[string]any{"phase": "setup", "source_head": strings.TrimSpace(string(head)), "wall_seconds": time.Since(setup).Seconds(), "groups": 4, "revisions_per_group": revisions})
	seed := time.Now()
	var workers sync.WaitGroup
	for group := 0; group < 4; group++ {
		name := fmt.Sprintf("replay-crash-%d", group)
		client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), topic, name)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer client.Close()
			for revision := 1; revision <= revisions; revision++ {
				consumerHistoryCommand(t, client, fmt.Sprintf("COMMIT_OFFSET topic=%s partition=0 group=%s offset=%d generation=%d member=%s", topic, name, revision, generation, member))
				if revision%5 == 0 {
					consumerHistoryCommand(t, client, fmt.Sprintf("HEARTBEAT topic=%s group=%s generation=%d member=%s", topic, name, generation, member))
				}
			}
			consumerHistoryCommand(t, client, fmt.Sprintf("LEAVE_GROUP topic=%s group=%s generation=%d member=%s", topic, name, generation, member))
		}()
	}
	workers.Wait()
	emit(map[string]any{"phase": "seed", "wall_seconds": time.Since(seed).Seconds(), "resources": historyMeasureResources(t), "metrics": historyMeasureMetrics(t)})
	const target = 1
	name := cleanupContainerName(target)
	historyMeasureDocker(t, "stop", name)
	before := historyMeasureResourcesFor(t, []int{2, 3})
	beforeMetrics := historyMeasureMetricsFor(t, []int{2, 3})
	began := time.Now()
	for attempt, threshold := range []int{1024, 2048, 1024} {
		historyMeasureOffline(t, target, "absent", "cold")
		historyReplayArm(t, target, threshold)
		start := time.Now()
		historyMeasureDocker(t, "start", name)
		require.NoError(t, eventually(t, "replay process exits at the armed read threshold", 2*time.Minute, func() (bool, string, error) {
			state := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.State.Running}} {{.State.ExitCode}}", name)))
			if strings.HasPrefix(state, "false ") && state != "false 77" {
				return false, state, fmt.Errorf("unexpected replay process exit: %s", state)
			}
			return state == "false 77", state, nil
		}))
		wall := time.Since(start).Seconds()
		fault := historyReplayStoppedJSON(t, target, "/app/cluster-logs/.consumer-replay-fault-result.json")
		require.GreaterOrEqual(t, fault["consumed_source_records"].(float64), float64(threshold))
		require.Equal(t, float64(threshold), fault["threshold_records"])
		// Assert absence before the next offline cache preparation can remove it.
		image := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.Image}}", name)))
		historyMeasureDocker(t, "run", "--rm", "--network", "none", "--pull", "never", "--volumes-from", name, "--entrypoint", "python3", image, "-c", "from pathlib import Path; assert not Path('/app/cluster-logs/.consumer-replay-index.json').exists(), 'incomplete replay published an index'")
		emit(map[string]any{"phase": "interrupted_replay", "attempt": attempt + 1, "wall_seconds": wall, "fault": fault, "exit_accounting": historyReplayStoppedJSON(t, target, "/tmp/history-exit.json")})
	}
	historyReplayArm(t, target, 0)
	finalStart := time.Now()
	historyMeasureDocker(t, "start", name)
	require.NoError(t, actions.waitForNodeHealth(target, fmt.Sprintf("http://localhost:%d/ready", healthPort(target))))
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "repeated replay process recovery")
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	for group := 0; group < 4; group++ {
		requireHistoryCommittedOffset(t, client, topic, fmt.Sprintf("replay-crash-%d", group), revisions)
	}
	emit(map[string]any{"phase": "final_verified_recovery", "wall_seconds": time.Since(finalStart).Seconds(), "all_attempts_wall_seconds": time.Since(began).Seconds(), "resources": historyMeasureResources(t), "metrics": historyMeasureMetrics(t), "survivors_before": before, "survivor_metrics_before": beforeMetrics})
	shutdown := time.Now()
	historyMeasureDocker(t, "stop", name)
	emit(map[string]any{"phase": "final_shutdown_accounting", "wall_seconds": time.Since(shutdown).Seconds(), "exit_accounting_includes_shutdown": historyReplayStoppedJSON(t, target, "/tmp/history-exit.json")})
}

func historyReplayArm(t *testing.T, node, threshold int) {
	t.Helper()
	name := cleanupContainerName(node)
	require.Equal(t, "false", strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.State.Running}}", name))))
	require.Equal(t, fmt.Sprintf("cursus-truncate-%d-e2e", cleanupPortOffset()), strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, name))))
	image := strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.Image}}", name)))
	script := "from pathlib import Path; import sys; root=Path('/app/cluster-logs'); (root/'.consumer-replay-fault-result.json').unlink(missing_ok=True); arm=root/'.consumer-replay-fault-arm'; arm.write_text(sys.argv[1]) if int(sys.argv[1]) else arm.unlink(missing_ok=True)"
	historyMeasureDocker(t, "run", "--rm", "--network", "none", "--pull", "never", "--volumes-from", name, "--entrypoint", "python3", image, "-c", script, strconv.Itoa(threshold))
}

func historyReplayStoppedJSON(t *testing.T, node int, source string) map[string]any {
	t.Helper()
	name := cleanupContainerName(node)
	require.Equal(t, "false", strings.TrimSpace(string(historyMeasureDocker(t, "inspect", "--format", "{{.State.Running}}", name))))
	path := filepath.Join(t.TempDir(), "result.json")
	historyMeasureDocker(t, "cp", name+":"+source, path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}
