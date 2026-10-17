package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clusterClient "github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func recoveryDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "%s", output)
	return strings.TrimSpace(string(output))
}

func publishRecoveryHistory(t *testing.T, addr, history string) {
	t.Helper()
	messages := make([]types.Message, 10000)
	for i := range messages {
		messages[i] = types.Message{ProducerID: "seed", SeqNum: uint64(i + 2), Payload: fmt.Sprintf("authoritative-%d", i)}
	}
	payload, err := util.EncodeBatchMessages(history, 0, "all", true, messages)
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	client, err := wire.NewClientConn(conn, "none")
	require.NoError(t, err)
	require.NoError(t, client.Send(payload))
	response, err := client.Receive()
	require.NoError(t, err)
	var ack struct {
		Status     string `json:"status"`
		LastOffset uint64 `json:"last_offset"`
	}
	require.NoError(t, json.Unmarshal(response, &ack))
	require.Equal(t, "OK", ack.Status, "%s", response)
	require.EqualValues(t, 10000, ack.LastOffset)
}

func readRecoveryHistory(t *testing.T, history string, node, leader, epoch int, end uint64) []types.Message {
	t.Helper()
	request := fsm.ReplicaCatchupRequest{
		Topic: history, BrokerID: fmt.Sprintf("broker-%d-9000", node%3+1),
		Leader: fmt.Sprintf("broker-%d-9000", leader), LeaderEpoch: epoch, LifecycleEpoch: 1,
		SourceBroker: fmt.Sprintf("broker-%d-9000", node), CommittedHWM: end, MaxRecords: fsm.MaxReplicaCatchupRecords,
	}
	transport := clusterClient.NewSecureTCPClusterClient("cursus-test-internal-token", nil)
	var records []types.Message
	for request.NextOffset < end {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		batch, err := transport.FetchReplicaCatchup(ctx, "localhost", 7999+node, request)
		cancel()
		require.NoError(t, err)
		require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(batch))
		require.Nil(t, batch.TruncateTo)
		records = append(records, batch.Messages...)
		require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
		if request.NextOffset == end {
			require.True(t, batch.Verified)
		}
	}
	return records
}

func TestIndexedRecoveryBoundsLargeLegacyTailOnThreeNodes(t *testing.T) {
	if os.Getenv("RUN_E2E_RETAINED_BOOTSTRAP") != "1" {
		t.Skip("set RUN_E2E_RETAINED_BOOTSTRAP=1 for indexed fault recovery")
	}
	const history = "indexed-tail-history"
	ctx := givenClusterRestart(t, "", composeFile, faultComposeFile, "docker-compose.recovery.yml").WithTopic(history).WithPartitions(1).WithAcks("all")
	actions := ctx.WhenCluster().StartCluster()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	_, err := client.SendCommand("", "CREATE topic="+history+" partitions=1 replication_factor=3 idempotent=true", 5*time.Second)
	require.NoError(t, err)
	actions.WaitForTopicMetadata()
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 1, 0, "committed-prefix", "all", true))
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "indexed fixture seed")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 1)
	description, err := client.SendCommand("", "DESCRIBE topic="+history, 5*time.Second)
	require.NoError(t, err)
	leader, err := leaderNodeFromDescribe(description, 3)
	require.NoError(t, err)
	offline := 1
	if leader == offline {
		offline = 2
	}
	container := fmt.Sprintf("%sbroker-%d", os.Getenv("CURSUS_E2E_CONTAINER_PREFIX"), offline)
	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-catchup-pause && touch /run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	imageID := recoveryDocker(t, "inspect", "--format={{.Image}}", container)
	recoveryDocker(t, "run", "--rm", "--volumes-from", container, "-e", "CURSUS_E2E_FAULTS=1", "--entrypoint", "/app/recovery-fixture", imageID)
	publishRecoveryHistory(t, ctx.GetBrokerAddrs()[leader-1], history)
	recoveryDocker(t, "start", container)
	local := e2e.NewBrokerClient([]string{ctx.GetBrokerAddrs()[offline-1]})
	defer local.Close()
	require.NoError(t, eventually(t, "uncommitted legacy fixture survives restart", clusterReadyTimeout, func() (bool, string, error) {
		response, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 3*time.Second)
		if err != nil {
			return false, response, nil
		}
		leo, _, err := parsePartitionOffsets(response, 0)
		return err == nil && leo == 10001, response, err
	}))
	before := recoveryWorkMetric(t, offline, "catchup_rpc", "transfer_bytes")
	readBefore := recoveryWorkMetric(t, offline, "lineage", "read_bytes")
	hashBefore := recoveryWorkMetric(t, offline, "lineage", "canonical_hash_bytes")
	previousLines := len(strings.Split(recoveryDocker(t, "logs", container), "\n"))
	const hold = "/run/cursus-e2e-faults/replica-response-hold/indexed-tail-history"
	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-response-hold && touch "+hold)
	recoveryDocker(t, "exec", container, "rm", "/run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
	require.NoError(t, eventually(t, "old response held before mutation", clusterReadyTimeout, func() (bool, string, error) {
		state := recoveryDocker(t, "exec", container, "sh", "-c", "if test -f "+hold+".entered; then echo held; else echo waiting; fi")
		return state == "held", state, nil
	}))
	newLeader := 6 - leader - offline // The other live member of this three-node cluster.
	newLeaderID := fmt.Sprintf("broker-%d-9000", newLeader)
	_, err = client.SendCommand("", fmt.Sprintf("ELECT_LEADER topic=%s partition=0 broker=%s", history, newLeaderID), 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, eventually(t, "receiver sees changed leader while response is held", clusterReadyTimeout, func() (bool, string, error) {
		response, err := local.SendCommand("admin", "CLUSTER_STATUS", 3*time.Second)
		if err != nil {
			return false, response, nil
		}
		var status struct {
			Partitions []struct{ Topic, Leader string } `json:"partitions"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
			return false, response, err
		}
		for _, partition := range status.Partitions {
			if partition.Topic == history {
				return partition.Leader == newLeaderID, partition.Leader, nil
			}
		}
		return false, "partition absent", nil
	}))
	// Pause subsequent fresh attempts so the rejected old response can be
	// inspected before a valid new response repairs the tail.
	recoveryDocker(t, "exec", container, "sh", "-c", "touch /run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history "+hold+".release")
	require.NoError(t, eventually(t, "old leader response rejected", clusterReadyTimeout, func() (bool, string, error) {
		logs := recoveryDocker(t, "logs", container)
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, `protocol=1 topic="indexed-tail-history"`) && strings.Contains(line, fmt.Sprintf(`source="broker-%d-9000"`, leader)) && strings.Contains(line, `error="replica recovery fence changed"`) {
				return true, "original response fenced", nil
			}
		}
		return false, "waiting for fenced response", nil
	}))
	response, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 3*time.Second)
	require.NoError(t, err)
	leo, _, err := parsePartitionOffsets(response, 0)
	require.NoError(t, err)
	require.EqualValues(t, 10001, leo, "old leader response must not truncate the local tail")
	t.Log("delayed old-leader response rejected before mutation")
	recoveryDocker(t, "exec", container, "rm", "/run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "large legacy tail recovery")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10001)
	bytes := recoveryWorkMetric(t, offline, "catchup_rpc", "transfer_bytes") - before
	require.Positive(t, bytes)
	require.Less(t, bytes, float64(8<<20), "recovery transfers the authoritative history with bounded protocol overhead")
	logs := recoveryDocker(t, "logs", container)
	exchanges := 0
	lines := strings.Split(logs, "\n")
	for _, line := range lines[min(previousLines, len(lines)):] {
		if !strings.Contains(line, `protocol=1 topic="indexed-tail-history"`) {
			continue
		}
		for _, field := range strings.Fields(line) {
			var count int
			if _, err := fmt.Sscanf(field, "exchanges=%d", &count); err == nil {
				exchanges += count
			}
		}
	}
	require.Positive(t, exchanges, "the indexed protocol must be observed")
	require.LessOrEqual(t, exchanges, 24)
	t.Logf("10000 divergent epoch-zero records repaired; indexed exchanges including failed turns=%d incremental response bytes=%.0f", exchanges, bytes)
	t.Logf("receiver lineage work after startup: read bytes=%.0f hash bytes=%.0f", recoveryWorkMetric(t, offline, "lineage", "read_bytes")-readBefore, recoveryWorkMetric(t, offline, "lineage", "canonical_hash_bytes")-hashBefore)
	epoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	want := readRecoveryHistory(t, history, leader, newLeader, epoch, 10001)
	require.Len(t, want, 10001)
	require.Equal(t, "committed-prefix", want[0].Payload)
	for i := 1; i < len(want); i++ {
		require.Equal(t, fmt.Sprintf("authoritative-%d", i-1), want[i].Payload)
	}
	for node := 1; node <= 3; node++ {
		require.Equal(t, want, readRecoveryHistory(t, history, node, newLeader, epoch, 10001))
	}
	// Exercise a second recovery with an abrupt exit after the durable truncate,
	// before producer/HWM reconstruction and incremental replay can complete.
	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-truncate-crash && touch /run/cursus-e2e-faults/replica-truncate-crash/indexed-tail-history-partition_0")
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	recoveryDocker(t, "run", "--rm", "--volumes-from", container, "-e", "CURSUS_E2E_FAULTS=1", "-e", "CURSUS_E2E_TAIL_START=10001", "--entrypoint", "/app/recovery-fixture", imageID)
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10002, 0, "after-truncate-crash", "all", true))
	recoveryDocker(t, "start", container)
	require.NoError(t, eventually(t, "process exits after durable truncation", clusterReadyTimeout, func() (bool, string, error) {
		state := recoveryDocker(t, "inspect", "--format={{.State.Running}}:{{.State.ExitCode}}", container)
		return state == "false:87", state, nil
	}))
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "interrupted truncation and incremental replay")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10002)
	got := readRecoveryHistory(t, history, offline, newLeader, epoch, 10002)
	require.Len(t, got, 10002)
	require.Equal(t, want, got[:10001])
	require.Equal(t, "after-truncate-crash", got[10001].Payload)

	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-catchup-crash && touch /run/cursus-e2e-faults/replica-catchup-crash/indexed-tail-history-0")
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10003, 0, "after-incremental-crash", "all", true))
	recoveryDocker(t, "start", container)
	require.NoError(t, eventually(t, "process exits after incremental append before HWM publication", 2*time.Minute, func() (bool, string, error) {
		state := recoveryDocker(t, "inspect", "--format={{.State.Running}}:{{.State.ExitCode}}", container)
		return state == "false:88", state, nil
	}))
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "interrupted incremental append")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10003)
	for node := 1; node <= 3; node++ {
		records := readRecoveryHistory(t, history, node, newLeader, epoch, 10003)
		require.Len(t, records, 10003)
		require.Equal(t, got, records[:10002])
		require.Equal(t, "after-incremental-crash", records[10002].Payload)
	}
}
