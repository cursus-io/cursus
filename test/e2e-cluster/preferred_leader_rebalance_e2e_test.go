package e2e_cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

const runPreferredLeaderRebalanceE2E = "RUN_E2E_LEADER_REBALANCE"

func TestPreferredLeaderReturnsWithoutReplicaRecovery(t *testing.T) {
	if os.Getenv(runPreferredLeaderRebalanceE2E) != "1" {
		t.Skipf("set %s=1 to run preferred leader recovery validation", runPreferredLeaderRebalanceE2E)
	}

	ctx := givenClusterRestart(t, "", composeFile, "docker-compose.leader-rebalance.yml").
		WithTopic("preferred-leader-recovery").
		WithPartitions(1).
		WithAcks("all").
		WithIdempotent(true)
	actions := ctx.WhenCluster()
	actions.CreateTopic()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "preferred leader fixture")

	initial := describePreferredLeaderTopic(t, ctx)
	preferredNode := replicaNode(t, initial.Partitions[0].Replicas[0])
	require.Equal(t, preferredNode, leaderNode(t, initial.Partitions[0].Leader), "new assignment must begin on its preferred leader")

	// Synchronize with the controller's periodic loop. The 60-second test
	// interval then leaves a full cycle for failover, catch-up, and baselining.
	controllerNode := waitForSingleRaftLeader(t, []int{1, 2, 3}, 0)
	cycleBefore := requireMetric(t, controllerNode, "cursus_leader_rebalance_cycle_duration_seconds_count")
	require.NoError(t, eventually(t, "preferred leader controller cycle", 75*time.Second, func() (bool, string, error) {
		value, err := fetchMetric(controllerNode, "cursus_leader_rebalance_cycle_duration_seconds_count")
		return err == nil && value > cycleBefore, fmt.Sprintf("cycles=%.0f before=%.0f", value, cycleBefore), err
	}))

	actions.StopBroker(preferredNode)
	waitForBrokerEvictedFromISR(t, ctx, preferredNode)
	require.NoError(t, eventually(t, "failover away from preferred leader", clusterReadyTimeout, func() (bool, string, error) {
		metadata, err := describePreferredLeaderTopicE(ctx)
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		current := leaderNodeE(metadata.Partitions[0].Leader)
		return current != 0 && current != preferredNode, fmt.Sprintf("leader=broker-%d preferred=broker-%d", current, preferredNode), nil
	}))

	survivors := make([]string, 0, 2)
	for node, addr := range ctx.GetBrokerAddrs() {
		if node+1 != preferredNode {
			survivors = append(survivors, addr)
		}
	}
	producer := e2e.NewBrokerClient(survivors)
	for sequence := uint64(0); sequence < 16; sequence++ {
		require.NoError(t, producer.PublishIdempotentToPartition(ctx.GetTopic(), "preferred-leader-e2e", 0, sequence, 0, fmt.Sprintf("event-%d", sequence), "all", true))
	}
	producer.Close()

	actions.StartBroker(preferredNode)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "preferred replica catch-up")
	beforeReturn := describePreferredLeaderTopic(t, ctx)
	require.NotEqual(t, preferredNode, leaderNode(t, beforeReturn.Partitions[0].Leader), "leader returned before the post-recovery work baseline")

	workBefore := recoveryByteWorkSnapshot(t)
	movesBefore := preferredLeaderAppliedMoves(t)
	require.NoError(t, eventually(t, "automatic return to preferred leader", 75*time.Second, func() (bool, string, error) {
		metadata, err := describePreferredLeaderTopicE(ctx)
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		current := leaderNodeE(metadata.Partitions[0].Leader)
		return current == preferredNode, fmt.Sprintf("leader=broker-%d preferred=broker-%d", current, preferredNode), nil
	}))

	final := describePreferredLeaderTopic(t, ctx)
	require.NoError(t, validateTopicMetadata(final, ctx.GetTopic(), 1, 3, 3, 16))
	require.GreaterOrEqual(t, preferredLeaderAppliedMoves(t), movesBefore+1)
	requireRecoveryByteWorkUnchanged(t, workBefore, recoveryByteWorkSnapshot(t))
}

func describePreferredLeaderTopic(t *testing.T, ctx *ClusterTestContext) topicMetadata {
	t.Helper()
	metadata, err := describePreferredLeaderTopicE(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Partitions, 1)
	return metadata
}

func describePreferredLeaderTopicE(ctx *ClusterTestContext) (topicMetadata, error) {
	response, err := ctx.GetClient().SendCommand("", "DESCRIBE topic="+ctx.GetTopic(), 5*time.Second)
	if err != nil {
		return topicMetadata{}, err
	}
	var metadata topicMetadata
	if err := json.Unmarshal([]byte(response), &metadata); err != nil {
		return topicMetadata{}, err
	}
	if len(metadata.Partitions) != 1 {
		return topicMetadata{}, fmt.Errorf("partitions=%d, want 1", len(metadata.Partitions))
	}
	return metadata, nil
}

func replicaNode(t *testing.T, brokerID string) int {
	t.Helper()
	var node int
	if _, err := fmt.Sscanf(brokerID, "broker-%d-9000", &node); err != nil || node < 1 || node > 3 {
		t.Fatalf("invalid replica broker ID %q", brokerID)
	}
	return node
}

func leaderNode(t *testing.T, address string) int {
	t.Helper()
	node := leaderNodeE(address)
	if node == 0 {
		t.Fatalf("invalid leader address %q", address)
	}
	return node
}

func leaderNodeE(address string) int {
	var node int
	host := strings.SplitN(address, ":", 2)[0]
	if _, err := fmt.Sscanf(host, "broker-%d", &node); err != nil || node < 1 || node > 3 || host != fmt.Sprintf("broker-%d", node) {
		return 0
	}
	return node
}

func requireMetric(t *testing.T, node int, name string) float64 {
	t.Helper()
	value, err := fetchMetric(node, name)
	require.NoError(t, err)
	return value
}

func preferredLeaderAppliedMoves(t *testing.T) float64 {
	t.Helper()
	var total float64
	for node := 1; node <= 3; node++ {
		metrics := scrapeMetricSamples(t, node, "cursus_leader_rebalance_moves_total")
		for sample, value := range metrics {
			if strings.Contains(sample, `result="success"`) && strings.Contains(sample, `reason="applied"`) {
				total += value
			}
		}
	}
	return total
}

func recoveryByteWorkSnapshot(t *testing.T) map[string]float64 {
	t.Helper()
	snapshot := make(map[string]float64)
	for node := 1; node <= 3; node++ {
		for sample, value := range scrapeMetricSamples(t, node, "cursus_recovery_work_total") {
			if strings.Contains(sample, `_bytes"}`) || strings.Contains(sample, `_bytes",`) {
				snapshot[fmt.Sprintf("broker-%d %s", node, sample)] = value
			}
		}
	}
	return snapshot
}

func requireRecoveryByteWorkUnchanged(t *testing.T, before, after map[string]float64) {
	t.Helper()
	for sample, afterValue := range after {
		require.Equal(t, before[sample], afterValue, "preferred leader election must not perform replica recovery work: %s", sample)
	}
	for sample, beforeValue := range before {
		require.Equal(t, beforeValue, after[sample], "preferred leader election reset recovery accounting: %s", sample)
	}
}

func scrapeMetricSamples(t *testing.T, node int, name string) map[string]float64 {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+clusterPortShift()+node)
	response, err := (&http.Client{Timeout: 3 * time.Second}).Get(url) // #nosec G107 -- fixed loopback exporter port.
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	require.Equal(t, http.StatusOK, response.StatusCode)

	values := make(map[string]float64)
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || (fields[0] != name && !strings.HasPrefix(fields[0], name+"{")) {
			continue
		}
		value, parseErr := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, parseErr)
		values[fields[0]] = value
	}
	require.NoError(t, scanner.Err())
	return values
}
