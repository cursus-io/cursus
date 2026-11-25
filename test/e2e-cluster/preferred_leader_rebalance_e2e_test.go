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

func TestReplicaRecoveryByteSampleClassification(t *testing.T) {
	tests := map[string]bool{
		`cursus_recovery_work_total{stage="catchup_rpc",unit="transfer_bytes"}`: true,
		`cursus_recovery_work_total{stage="lineage",unit="transfer_bytes"}`:     true,
		`cursus_recovery_work_total{stage="chunk_read",unit="read_bytes"}`:      true,
		`cursus_recovery_work_total{stage="bootstrap_file",unit="write_bytes"}`: true,
		`cursus_recovery_work_total{stage="lineage",unit="read_bytes"}`:         false,
		`cursus_recovery_work_total{stage="retained_page",unit="write_bytes"}`:  false,
		`cursus_recovery_work_total{stage="catchup_rpc",unit="decoded_bytes"}`:  false,
	}
	for sample, expected := range tests {
		require.Equal(t, expected, isReplicaRecoveryByteSample(sample), sample)
	}
}

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
	survivorNodes := make([]int, 0, 2)
	for node, addr := range ctx.GetBrokerAddrs() {
		if node+1 != preferredNode {
			survivors = append(survivors, addr)
			survivorNodes = append(survivorNodes, node+1)
		}
	}
	producer := e2e.NewBrokerClient(survivors)
	producerEpoch := time.Now().UnixNano()
	for sequence := uint64(1); sequence <= 16; sequence++ {
		require.NoError(t, eventually(t, fmt.Sprintf("idempotent publish after failover sequence %d", sequence), clusterReadyTimeout, func() (bool, string, error) {
			err := producer.PublishIdempotentToPartition(ctx.GetTopic(), "preferred-leader-e2e", 0, sequence, producerEpoch, fmt.Sprintf("event-%d", sequence), "all", true)
			return err == nil, fmt.Sprintf("sequence=%d err=%v", sequence, err), nil
		}))
	}
	producer.Close()

	// Synchronize while the preferred broker is ineligible. The current Raft
	// leader cannot move the partition during this cycle, and the next 120-second
	// interval starts before recovery and the fixed work baseline.
	controllerNode := waitForSingleRaftLeader(t, survivorNodes, 0)
	cycleBefore := requireMetric(t, controllerNode, "cursus_leader_rebalance_cycle_duration_seconds_count")
	cycleBoundary := cycleBefore
	require.NoError(t, eventually(t, "preferred leader controller cycle", 135*time.Second, func() (bool, string, error) {
		value, err := fetchMetric(controllerNode, "cursus_leader_rebalance_cycle_duration_seconds_count")
		if err == nil && value > cycleBefore {
			cycleBoundary = value
		}
		return err == nil && value > cycleBefore, fmt.Sprintf("cycles=%.0f before=%.0f", value, cycleBefore), err
	}))

	actions.StartBroker(preferredNode)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "preferred replica catch-up")
	require.Equal(t, controllerNode, waitForSingleRaftLeader(t, []int{1, 2, 3}, 0), "Raft leadership changed before the recovery baseline")
	require.Equal(t, cycleBoundary, requireMetric(t, controllerNode, "cursus_leader_rebalance_cycle_duration_seconds_count"), "rebalance cycle ran before the recovery baseline")
	beforeReturn := describePreferredLeaderTopic(t, ctx)
	require.NotEqual(t, preferredNode, leaderNode(t, beforeReturn.Partitions[0].Leader), "leader returned before the post-recovery work baseline")

	workBeforeMove := recoveryByteWorkSnapshot(t)
	movesBefore := preferredLeaderAppliedMoves(t)
	require.NoError(t, eventually(t, "automatic return to preferred leader and move metric", 135*time.Second, func() (bool, string, error) {
		metadata, err := describePreferredLeaderTopicE(ctx)
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		current := leaderNodeE(metadata.Partitions[0].Leader)
		if current != preferredNode {
			return false, fmt.Sprintf("leader=broker-%d preferred=broker-%d", current, preferredNode), nil
		}
		moves := preferredLeaderAppliedMoves(t)
		return moves >= movesBefore+1, fmt.Sprintf("leader=broker-%d preferred=broker-%d moves=%.0f before=%.0f", current, preferredNode, moves, movesBefore), nil
	}))

	final := describePreferredLeaderTopic(t, ctx)
	require.NoError(t, validateTopicMetadata(final, ctx.GetTopic(), 1, 3, 3, 16))
	workAfterMove := recoveryByteWorkSnapshot(t)
	logRecoveryByteWorkDelta(t, workBeforeMove, workAfterMove)
	requireReplicaRecoveryByteWorkUnchanged(t, workBeforeMove, workAfterMove)
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
			if strings.Contains(sample, `unit="read_bytes"`) || strings.Contains(sample, `unit="write_bytes"`) || strings.Contains(sample, `unit="transfer_bytes"`) {
				snapshot[fmt.Sprintf("broker-%d %s", node, sample)] = value
			}
		}
	}
	return snapshot
}

func logRecoveryByteWorkDelta(t *testing.T, before, after map[string]float64) {
	t.Helper()
	for sample, afterValue := range after {
		if delta := afterValue - before[sample]; delta != 0 {
			t.Logf("preferred leader move recovery work delta: %s delta=%.0f", sample, delta)
		}
	}
}

func requireReplicaRecoveryByteWorkUnchanged(t *testing.T, before, after map[string]float64) {
	t.Helper()
	for sample, afterValue := range after {
		if !isReplicaRecoveryByteSample(sample) {
			continue
		}
		require.Equal(t, before[sample], afterValue, "preferred leader election must not perform replica data recovery work: %s", sample)
	}
	for sample, beforeValue := range before {
		if !isReplicaRecoveryByteSample(sample) {
			continue
		}
		require.Equal(t, beforeValue, after[sample], "preferred leader election reset recovery accounting: %s", sample)
	}
}

func isReplicaRecoveryByteSample(sample string) bool {
	if strings.Contains(sample, `unit="transfer_bytes"`) {
		return true
	}
	if !strings.Contains(sample, `unit="read_bytes"`) && !strings.Contains(sample, `unit="write_bytes"`) {
		return false
	}
	for _, stage := range []string{"full_export", "retained_export", "chunk_read", "chunk_receive", "detached_copy", "detached_read", "replica_rpc", "catchup_rpc", "bootstrap_file", "bootstrap_install"} {
		if strings.Contains(sample, `stage="`+stage+`"`) {
			return true
		}
	}
	return false
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
