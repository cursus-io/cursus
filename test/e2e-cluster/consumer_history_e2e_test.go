package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// This scenario requires its own project, ports, network and named volumes.
// Never allow the opt-in flag alone to target the default developer cluster.
func TestConsumerHistoryCompactionRecovery(t *testing.T) {
	if os.Getenv("RUN_E2E_CONSUMER_HISTORY") != "1" {
		t.Skip("set RUN_E2E_CONSUMER_HISTORY=1 with CURSUS_CLEANUP_PORT_OFFSET")
	}
	require.NotZero(t, cleanupPortOffset(), "an isolated port/project offset is mandatory")
	files := []string{composeFile, "docker-compose.consumer-history.yml"}
	if os.Getenv("CURSUS_HISTORY_REPLICATION_FACTOR") == "2" {
		t.Setenv("CURSUS_HISTORY_MEASURE_COMPACTION", "true")
		t.Setenv("CURSUS_HISTORY_TRACE_SYNC", "false")
		files = append(files, "docker-compose.history-measure.yml")
	}
	ctx := givenCleanupCluster(t, files...).WithClusterSize(3).WithTopic("history-orders").WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	defer historyFailureDiagnostics(t, ctx.GetBrokerAddrs())
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-orders partitions=1 replication_factor=3")
	metadataJSON, err := ctx.GetClient().SendCommand("", "DESCRIBE topic=__consumer_offsets", 5*time.Second)
	require.NoError(t, err)
	var metadata topicMetadata
	require.NoError(t, json.Unmarshal([]byte(metadataJSON), &metadata))
	replicas := make(map[int]bool)
	remoteReaders := make(map[int]bool)
	for _, partition := range metadata.Partitions {
		if os.Getenv("CURSUS_HISTORY_REPLICATION_FACTOR") == "2" {
			require.Len(t, partition.Replicas, 2)
		}
		for node := 1; node <= 3; node++ {
			if slices.Contains(partition.Replicas, fmt.Sprintf("broker-%d-9000", node)) {
				replicas[node] = true
			} else {
				remoteReaders[node] = true
			}
		}
	}
	require.NotEmpty(t, replicas)
	if os.Getenv("CURSUS_HISTORY_REPLICATION_FACTOR") == "2" {
		require.NotEmpty(t, remoteReaders, "fixture must include an actual nonreplica reader")
	}
	t.Logf("internal metadata replica brokers=%v", replicas)
	var heartbeat []func()
	var commit []func(uint64)
	var fetch []func() uint64
	var leave []func()
	var commitTimes []time.Duration
	availabilityRetries := 0
	for i := 0; i < 4; i++ {
		group := fmt.Sprintf("history-group-%d", i)
		client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-orders", group)
		defer client.Close()
		heartbeat = append(heartbeat, func() {
			availabilityRetries += consumerHistoryCommand(t, client, fmt.Sprintf("HEARTBEAT topic=history-orders group=%s member=%s generation=%d", group, member, generation))
		})
		commit = append(commit, func(offset uint64) {
			start := time.Now()
			availabilityRetries += consumerHistoryCommand(t, client, fmt.Sprintf("COMMIT_OFFSET topic=history-orders partition=0 group=%s offset=%d generation=%d member=%s", group, offset, generation, member))
			commitTimes = append(commitTimes, time.Since(start))
		})
		fetch = append(fetch, func() uint64 { return fetchHistoryOffset(t, client, "history-orders", group) })
		leave = append(leave, func() {
			consumerHistoryCommand(t, client, fmt.Sprintf("LEAVE_GROUP topic=history-orders group=%s member=%s generation=%d", group, member, generation))
		})
	}
	start := time.Now()
	for revision := 1; revision <= 100; revision++ {
		for _, write := range commit {
			write(uint64(revision))
		}
		if revision%5 == 0 {
			for _, beat := range heartbeat {
				beat()
			}
		}
	}
	t.Logf("four fixed groups: 400 acknowledged commits wall=%s", time.Since(start))
	slices.Sort(commitTimes)
	t.Logf("commit completion including availability retries: p50=%s p95=%s max=%s retries=%d", commitTimes[len(commitTimes)/2], commitTimes[len(commitTimes)*95/100], commitTimes[len(commitTimes)-1], availabilityRetries)
	require.NoError(t, eventually(t, "certified history removed on every replica", 2*time.Minute, func() (bool, string, error) {
		for _, beat := range heartbeat {
			beat()
		}
		for node := 1; node <= 3; node++ {
			if !replicas[node] {
				continue // A nonreplica has no authoritative files to compact.
			}
			count, err := consumerHistoryRemoved(node)
			if err != nil {
				return false, "metrics", err
			}
			if count < 20 {
				return false, fmt.Sprintf("node=%d removed=%v", node, count), nil
			}
		}
		return true, "all replicas compacted", nil
	}))
	for _, read := range fetch {
		require.Equal(t, uint64(100), read())
	}
	// End sessions explicitly before the restart measurement; abandoned members
	// would expire at unrelated times while three brokers restart in sequence.
	for _, leaveGroup := range leave {
		leaveGroup()
	}
	for node := 1; node <= 3; node++ {
		name := cleanupContainerName(node)
		output, err := exec.Command("docker", "inspect", "--format", "{{.Image}}", name).CombinedOutput()
		require.NoError(t, err, string(output))
		imageID := strings.TrimSpace(string(output))
		output, err = exec.Command("docker", "stop", name).CombinedOutput()
		require.NoError(t, err, string(output))
		// Remove the disposable cache only after the writer has stopped. Reuse
		// this fixture's exact image and volume, with no network or image pull.
		output, err = exec.Command("docker", "run", "--rm", "--network", "none", "--pull", "never",
			"--volumes-from", name, "--entrypoint", "/bin/sh", imageID, "-c",
			"rm -f /app/cluster-logs/.consumer-replay-index.json && test ! -e /app/cluster-logs/.consumer-replay-index.json").CombinedOutput()
		require.NoError(t, err, string(output))
		// The measured start excludes shutdown and explicit cache removal.
		start = time.Now()
		output, err = exec.Command("docker", "start", name).CombinedOutput()
		require.NoError(t, err, string(output))
		require.NoError(t, actions.waitForNodeHealth(node, fmt.Sprintf("http://localhost:%d/ready", healthPort(node))))
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, "history cache-miss restart")
		t.Logf("node=%d cache-absent-start-to-ready-and-full-ISR=%s OS cache not dropped; shutdown/cache removal excluded", node, time.Since(start))
	}
	for i := 0; i < 4; i++ {
		group := fmt.Sprintf("history-group-%d", i)
		client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-orders", group)
		require.Equal(t, uint64(100), fetchHistoryOffset(t, client, "history-orders", group))
		consumerHistoryCommand(t, client, fmt.Sprintf("LEAVE_GROUP topic=history-orders group=%s member=%s generation=%d", group, member, generation))
		client.Close()
	}
	if os.Getenv("CURSUS_HISTORY_REPLICATION_FACTOR") == "2" {
		for node := 1; node <= 3; node++ {
			if !remoteReaders[node] {
				continue
			}
			values := historyMeasureMetricsFor(t, []int{node})[node]
			transferred := values[`cursus_recovery_work_total{stage="metadata_rpc",unit="transfer_bytes"}`]
			require.Positive(t, transferred, "nonreplica restart must exercise the remote metadata source")
			t.Logf("nonreplica node=%d metadata transfer logical bytes=%v", node, transferred)
		}
	}
	// Exercise lifecycle cleanup while the activated cleaner keeps running.
	// The unrelated member must retain the same identity and generation.
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-unrelated partitions=1 replication_factor=3")
	live, liveGeneration, liveMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-unrelated", "history-live")
	defer live.Close()
	consumerHistoryCommand(t, live, fmt.Sprintf("COMMIT_OFFSET topic=history-unrelated partition=0 group=history-live offset=17 generation=%d member=%s", liveGeneration, liveMember))
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "TRUNCATE topic=history-orders expected_revision=1")
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	for _, operation := range []string{"truncate", "delete"} {
		if operation == "delete" {
			sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic=history-orders")
			requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
			sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-orders partitions=1 replication_factor=3")
		}
		consumerHistoryCommand(t, live, fmt.Sprintf("HEARTBEAT topic=history-unrelated group=history-live member=%s generation=%d", liveMember, liveGeneration))
		require.Equal(t, uint64(17), fetchHistoryOffset(t, live, "history-unrelated", "history-live"))
		client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-orders", "history-group-0")
		require.Zero(t, fetchHistoryOffset(t, client, "history-orders", "history-group-0"), "%s must not resurrect compacted offsets", operation)
		consumerHistoryCommand(t, client, fmt.Sprintf("COMMIT_OFFSET topic=history-orders partition=0 group=history-group-0 offset=1 generation=%d member=%s", generation, member))
		require.Equal(t, uint64(1), fetchHistoryOffset(t, client, "history-orders", "history-group-0"))
		consumerHistoryCommand(t, client, fmt.Sprintf("LEAVE_GROUP topic=history-orders group=history-group-0 member=%s generation=%d", member, generation))
		client.Close()
	}
	consumerHistoryCommand(t, live, fmt.Sprintf("LEAVE_GROUP topic=history-unrelated group=history-live member=%s generation=%d", liveMember, liveGeneration))
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic=history-unrelated")
}

// Publication briefly closes admission. Count those responses and include their
// retry cost in completion latency; never rejoin or forgive a member/epoch error.
func consumerHistoryCommand(t *testing.T, client *e2e.BrokerClient, command string) int {
	t.Helper()
	return retryHistoryAdmission(t, func() error {
		response, err := client.SendCommand("", command, 5*time.Second)
		if err == nil {
			require.True(t, strings.HasPrefix(response, "OK"), response)
		}
		return err
	})
}

func fetchHistoryOffset(t *testing.T, client *e2e.BrokerClient, topic, group string) uint64 {
	t.Helper()
	var offset uint64
	retryHistoryAdmission(t, func() error {
		var err error
		offset, err = client.FetchCommittedOffset(topic, 0, group)
		return err
	})
	return offset
}

func historyFailureDiagnostics(t *testing.T, addrs []string) {
	t.Helper()
	if !t.Failed() {
		return
	}
	for _, addr := range addrs {
		client := e2e.NewBrokerClient([]string{addr})
		response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
		client.Close()
		t.Logf("history failure broker=%s status=%s error=%v", addr, response, err)
	}
}

func consumerHistoryRemoved(node int) (float64, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://localhost:%d/metrics", baseExporterPort+clusterPortShift()+node))
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "cursus_consumer_history_removed_records_total" {
			return strconv.ParseFloat(fields[1], 64)
		}
	}
	return 0, fmt.Errorf("consumer history metric missing")
}
