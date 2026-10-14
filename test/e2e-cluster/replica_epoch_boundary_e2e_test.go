package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clusterClient "github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// TestReplicaRecoveryWithoutLaterEpochBoundary verifies a divergent follower
// rejoins ISR without a later source epoch and retains its repaired log on restart.
func TestReplicaRecoveryWithoutLaterEpochBoundary(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run missing epoch boundary recovery validation")
	}
	const history = "epoch-boundary-history"
	ctx := GivenFaultClusterRestart(t).WithClusterSize(3).WithTopic(history).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+history+" partitions=1 replication_factor=3")
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), history, map[string]string{
		"revision": "1", "lifecycle_epoch": "1", "replication_factor": "3",
	})
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 0)
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 1, 0, "common", "all", true))
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 1)
	epoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	require.Positive(t, epoch)
	for node := 1; node <= 3; node++ {
		setReplicaFaultSentinel(t, node, "replica-append-failure", history+"-0-1", true)
		setReplicaCatchupPauseFault(t, node, history, true)
		defer setReplicaCatchupPauseFaultBestEffort(node, history, false)
	}
	_, err := client.SendCommand("", "PUBLISH topic="+history+" partition=0 acks=all producerId=failed message=divergent", 15*time.Second)
	require.ErrorContains(t, err, "replica_index_failed")
	divergentNode := 0
	for index, addr := range ctx.GetBrokerAddrs() {
		local := e2e.NewBrokerClient([]string{addr})
		response, readErr := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 5*time.Second)
		local.Close()
		require.NoError(t, readErr)
		leo, hwm, readErr := parsePartitionOffsets(response, 0)
		require.NoError(t, readErr)
		require.Equal(t, uint64(1), hwm)
		if leo == 2 {
			require.Zero(t, divergentNode, "only one follower should retain the failed append")
			divergentNode = index + 1
		} else {
			require.Equal(t, uint64(1), leo)
		}
		setReplicaFaultSentinel(t, index+1, "replica-append-failure", history+"-0-1", false)
	}
	require.NotZero(t, divergentNode)
	actions.StopBroker(divergentNode)
	available := availableBrokerAddrs(ctx.GetBrokerAddrs(), divergentNode)
	requireFailoverISRReady(t, available, history, divergentNode, 1)
	live := e2e.NewBrokerClient(available)
	defer live.Close()
	require.NoError(t, live.PublishIdempotentToPartition(history, "replacement", 0, 1, 0, "authoritative", "all", true))
	require.NoError(t, live.PublishIdempotentToPartition(history, "replacement", 0, 2, 0, "after-replacement", "all", true))
	requireReplicaOffsetsEventually(t, available, history, 3)
	requirePartitionRecoveryState(t, available, history, 0, false, 2, epoch)

	var committed []types.Message
	for node := 1; node <= 3; node++ {
		if node == divergentNode {
			continue
		}
		committed = readReplicaEpochBoundaryRecords(t, available, history, node, epoch)
		for _, record := range committed {
			require.Equal(t, int64(epoch), record.LeaderEpoch, "source must have no later epoch boundary")
		}
	}
	require.Len(t, committed, 3)
	restartBrokerDuringRecovery(t, divergentNode)
	// Keep catch-up paused until the restarted follower exposes its old tail.
	require.NoError(t, eventually(t, "restarted divergent tail", clusterReadyTimeout, func() (bool, string, error) {
		local := e2e.NewBrokerClient([]string{ctx.GetBrokerAddrs()[divergentNode-1]})
		defer local.Close()
		response, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 5*time.Second)
		if err != nil {
			return false, response, err
		}
		leo, _, err := parsePartitionOffsets(response, 0)
		return err == nil && leo == 2, response, err
	}))
	code, _, err := readReadiness(divergentNode)
	require.NoError(t, err)
	require.NotEqual(t, 200, code, "unverified follower must not be ready")
	for node := 1; node <= 3; node++ {
		setReplicaCatchupPauseFault(t, node, history, false)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "missing epoch boundary recovery")
	requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, epoch)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 3)
	for node := 1; node <= 3; node++ {
		require.Equal(t, committed, readReplicaEpochBoundaryRecords(t, ctx.GetBrokerAddrs(), history, node, epoch))
		code, _, err := readReadiness(node)
		require.NoError(t, err)
		require.Equal(t, 200, code)
	}
	// The repaired log must remain durable through another follower restart.
	actions.StopBroker(divergentNode)
	actions.StartBroker(divergentNode)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "repaired follower restart")
	require.Equal(t, committed, readReplicaEpochBoundaryRecords(t, ctx.GetBrokerAddrs(), history, divergentNode, epoch))
	require.NoError(t, client.PublishIdempotentToPartition(history, "replacement", 0, 3, 0, "resumed", "all", true))
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 4)
}

// readReplicaEpochBoundaryRecords reads authenticated raw records from the
// selected ISR member so replica equality includes persisted recovery metadata.
func readReplicaEpochBoundaryRecords(t *testing.T, addrs []string, topicName string, node, epoch int) []types.Message {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
	require.NoError(t, err)
	var status struct {
		Partitions []struct {
			Topic        string `json:"topic"`
			Leader       string `json:"leader"`
			CommittedHWM uint64 `json:"committed_hwm"`
		} `json:"partitions"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status))
	for _, partition := range status.Partitions {
		if partition.Topic != topicName {
			continue
		}
		request := fsm.ReplicaCatchupRequest{
			Topic: topicName, Partition: 0, BrokerID: fmt.Sprintf("broker-%d-9000", node%3+1),
			Leader: partition.Leader, LeaderEpoch: epoch, LifecycleEpoch: 1,
			SourceBroker: fmt.Sprintf("broker-%d-9000", node),
			CommittedHWM: partition.CommittedHWM, MaxRecords: fsm.MaxReplicaCatchupRecords,
		}
		fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		transport := clusterClient.NewSecureTCPClusterClient("cursus-test-internal-token", nil)
		batch, err := transport.FetchReplicaCatchup(fetchCtx, "localhost", 7999+node, request)
		require.NoError(t, err)
		require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(batch))
		require.True(t, batch.Verified)
		return batch.Messages
	}
	t.Fatalf("partition missing for %s", topicName)
	return nil
}
