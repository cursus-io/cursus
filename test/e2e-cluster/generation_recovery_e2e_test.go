package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Read-only evidence: hashing the persisted segments also detects a same-length
// rewrite that LIST_OFFSETS alone cannot detect. Sidecars are derived evidence.
func generationRecoveryLogDigest(t *testing.T, container, history string) string {
	t.Helper()
	require.Equal(t, "indexed-tail-history", history)
	digest := recoveryDocker(t, "exec", container, "sh", "-c", "sha256sum /app/cluster-logs/indexed-tail-history/partition_0_segment_*.log")
	require.NotEmpty(t, digest)
	return digest
}

func requireGenerationReplicaExcluded(t *testing.T, addrs []string, history string, offline int, hwm uint64) {
	t.Helper()
	for _, addr := range addrs {
		client := e2e.NewBrokerClient([]string{addr})
		response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
		client.Close()
		require.NoError(t, err)
		var status struct {
			Partitions []struct {
				Topic        string   `json:"topic"`
				ISR          []string `json:"isr"`
				CommittedHWM uint64   `json:"committed_hwm"`
			} `json:"partitions"`
		}
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status))
		found := false
		for _, partition := range status.Partitions {
			if partition.Topic == history {
				found = true
				require.Equal(t, hwm, partition.CommittedHWM)
				require.Len(t, partition.ISR, 2)
				require.NotContains(t, partition.ISR, fmt.Sprintf("broker-%d-9000", offline))
			}
		}
		require.True(t, found, "partition absent on %s", addr)
	}
	// /ready publishes a periodic observation, not an inline metadata read.
	// Keep recovery held while waiting for the new HWM to reach that sample.
	require.NoError(t, eventually(t, "unverified replica observed unready", 10*time.Second, func() (bool, string, error) {
		code, body, err := readReadiness(offline)
		return err == nil && code == 503, fmt.Sprintf("status=%d body=%s", code, body), err
	}))
}

type generationWorkKey struct {
	node        int
	stage, unit string
}

func captureGenerationRecoveryWork(t *testing.T) map[generationWorkKey]float64 {
	t.Helper()
	values := make(map[generationWorkKey]float64)
	for node := 1; node <= 3; node++ {
		for _, stage := range []string{"lineage", "retained_scan", "catchup_rpc"} {
			for _, unit := range []string{"read_bytes", "decoded_frames", "canonical_hash_bytes", "transfer_bytes"} {
				values[generationWorkKey{node, stage, unit}] = recoveryWorkMetric(t, node, stage, unit)
			}
		}
	}
	return values
}

func logGenerationRecoveryWork(t *testing.T, before map[generationWorkKey]float64) {
	t.Helper()
	after := captureGenerationRecoveryWork(t)
	for node := 1; node <= 3; node++ {
		for _, stage := range []string{"lineage", "retained_scan", "catchup_rpc"} {
			deltas := make(map[string]float64)
			for _, unit := range []string{"read_bytes", "decoded_frames", "canonical_hash_bytes", "transfer_bytes"} {
				key := generationWorkKey{node, stage, unit}
				delta := after[key] - before[key]
				require.GreaterOrEqual(t, delta, float64(0), "measurement must not cross process restarts")
				deltas[unit] = delta
			}
			t.Logf("generation recovery work: node=%d stage=%s read_bytes=%.0f records=%.0f canonical_hash_bytes=%.0f transfer_bytes=%.0f", node, stage, deltas["read_bytes"], deltas["decoded_frames"], deltas["canonical_hash_bytes"], deltas["transfer_bytes"])
		}
	}
}

func restartGenerationRecoveryCluster(t *testing.T, ctx *ClusterTestContext, actions *ClusterActions) {
	t.Helper()
	// Stop all before starting any. Restart, do not recreate/delete their volumes.
	prefix := os.Getenv("CURSUS_E2E_CONTAINER_PREFIX")
	brokers := []string{prefix + "broker-1", prefix + "broker-2", prefix + "broker-3"}
	recoveryDocker(t, append([]string{"stop"}, brokers...)...)
	ctx.GetClient().Close()
	recoveryDocker(t, append([]string{"start"}, brokers...)...)
	for node := 1; node <= 3; node++ {
		require.NoError(t, actions.waitForNodeHealthWithin(node, clusterHealthCheckAddrs(3)[node-1], 2*time.Minute))
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "all brokers restarted with durable data")
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
}

const generationEvents = "generation-recovery-events"

func appendGenerationEvent(t *testing.T, addrs []string, version int) string {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("", fmt.Sprintf("APPEND_STREAM topic=%s key=order version=%d event_id=generation-event-%d producerId=event-writer seqNum=%d event_type=Updated schema_version=1 message=event-%d", generationEvents, version, version, version, version), 15*time.Second)
	require.NoError(t, err)
	require.Contains(t, response, fmt.Sprintf("OK version=%d offset=%d partition=0", version, version-1))
	return response
}

func verifyGenerationEvents(t *testing.T, addrs []string, responses []string) {
	t.Helper()
	for i, response := range responses {
		require.Equal(t, response, appendGenerationEvent(t, addrs, i+1), "replay must suppress event-ID duplicates")
	}
	admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, RequestTimeoutMS: 3000, MaxRetries: 5, RetryBackoffMS: 100})
	require.NoError(t, err)
	requestCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	history, err := admin.ReadStreamHistory(requestCtx, sdk.HistoryRequest{Topic: generationEvents, Key: "order", FromVersion: 1, MaxRecords: 32, MaxBytes: 65536})
	require.NoError(t, err)
	require.Equal(t, sdk.HistoryComplete, history.Completeness)
	require.False(t, history.HasMore)
	require.Len(t, history.Events, len(responses))
	for i, event := range history.Events {
		require.Equal(t, uint64(i+1), event.Version)
		require.Equal(t, fmt.Sprintf("event-%d", i+1), event.Payload)
	}
	requireReplicaOffsetsEventually(t, addrs, generationEvents, uint64(len(responses)))
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	description, err := client.SendCommand("", "DESCRIBE topic="+generationEvents, 5*time.Second)
	require.NoError(t, err)
	leader, err := leaderNodeFromDescribe(description, 3)
	require.NoError(t, err)
	epoch := requirePartitionRecoveryState(t, addrs, generationEvents, 0, false, 3, 0)
	want := readRecoveryHistory(t, generationEvents, leader, leader, epoch, uint64(len(responses)))
	for node := 1; node <= 3; node++ {
		require.Equal(t, want, readRecoveryHistory(t, generationEvents, node, leader, epoch, uint64(len(responses))))
	}
}
