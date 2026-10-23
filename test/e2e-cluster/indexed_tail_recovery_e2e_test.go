package e2e_cluster

import (
	"context"
	"encoding/json"
	"errors"
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

func publishRecoveryHistory(t *testing.T, addr, history string, afterBatch func(uint64)) {
	t.Helper()
	for start := 0; start < 10000; start += 1000 {
		messages := make([]types.Message, 1000)
		for i := range messages {
			messages[i] = types.Message{ProducerID: "seed", SeqNum: uint64(start + i + 2), Payload: fmt.Sprintf("authoritative-%d", start+i)}
		}
		payload, err := util.EncodeBatchMessages(history, 0, "all", true, messages)
		require.NoError(t, err)
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		require.NoError(t, err)
		require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
		client, err := wire.NewClientConn(conn, "none")
		require.NoError(t, err)
		require.NoError(t, client.Send(payload))
		response, err := client.Receive()
		_ = conn.Close()
		require.NoError(t, err)
		var ack struct {
			Status     string `json:"status"`
			LastOffset uint64 `json:"last_offset"`
		}
		require.NoError(t, json.Unmarshal(response, &ack))
		require.Equal(t, "OK", ack.Status, "%s", response)
		require.EqualValues(t, start+1000, ack.LastOffset)
		afterBatch(ack.LastOffset + 1)
	}
}

func readRecoveryHistory(t *testing.T, history string, node, leader, epoch int, end uint64) []types.Message {
	t.Helper()
	request := fsm.ReplicaCatchupRequest{
		Topic: history, BrokerID: fmt.Sprintf("broker-%d-9000", node%3+1),
		Leader: fmt.Sprintf("broker-%d-9000", leader), LeaderEpoch: epoch, LifecycleEpoch: 1,
		SourceBroker: fmt.Sprintf("broker-%d-9000", node), CommittedHWM: end, MaxRecords: fsm.MaxReplicaCatchupRecords,
	}
	transport := clusterClient.NewSecureTCPClusterClient("cursus-test-internal-token", nil)
	readCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	var records []types.Message
	for request.NextOffset < end {
		ctx, cancel := context.WithTimeout(readCtx, 5*time.Second)
		batch, err := transport.FetchReplicaCatchup(ctx, "localhost", 7999+node, request)
		cancel()
		var busy *wire.BrokerError
		if errors.As(err, &busy) && busy.Code == "recovery_busy" && busy.Retryable {
			// This inspection competes with production recovery on the same
			// source budget. Only explicit quota contention is retryable here.
			select {
			case <-readCtx.Done():
				require.NoError(t, readCtx.Err(), "source admission did not recover")
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
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
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+generationEvents+" partitions=1 replication_factor=3 aggregate_replay=true")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), generationEvents, 0)
	eventResponses := []string{appendGenerationEvent(t, ctx.GetBrokerAddrs(), 1)}
	verifyGenerationEvents(t, ctx.GetBrokerAddrs(), eventResponses)
	description, err := client.SendCommand("", "DESCRIBE topic="+history, 5*time.Second)
	require.NoError(t, err)
	leader, err := leaderNodeFromDescribe(description, 3)
	require.NoError(t, err)
	seedEpoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	committedSeed := readRecoveryHistory(t, history, leader, leader, seedEpoch, 1)
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
	const group = "generation-recovery-offsets"
	available := availableBrokerAddrs(ctx.GetBrokerAddrs(), offline)
	reader, generation, member := joinRecoveryGroup(t, available, history, group)
	defer reader.Close()
	commit := func(offset uint64) {
		commitGroupOffset(t, reader, history, group, offset, generation, member)
		heartbeatGroup(t, reader, history, group, generation, member)
		require.Equal(t, offset, fetchCommittedOffset(t, reader, history, group))
	}
	publishRecoveryHistory(t, ctx.GetBrokerAddrs()[leader-1], history, commit)
	eventResponses = append(eventResponses, appendGenerationEvent(t, available, 2))
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
	workBefore := captureGenerationRecoveryWork(t)
	previousLines := len(strings.Split(recoveryDocker(t, "logs", container), "\n"))
	const hold = "/run/cursus-e2e-faults/replica-response-hold/indexed-tail-history"
	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-response-hold && touch "+hold)
	recoveryDocker(t, "exec", container, "rm", "/run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
	require.NoError(t, eventually(t, "old response held before mutation", clusterReadyTimeout, func() (bool, string, error) {
		state := recoveryDocker(t, "exec", container, "sh", "-c", "if test -f "+hold+".entered; then echo held; else echo waiting; fi")
		return state == "held", state, nil
	}))
	unchangedLog := generationRecoveryLogDigest(t, container, history)
	heldOffsets, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 3*time.Second)
	require.NoError(t, err)
	heldLEO, heldHWM, err := parsePartitionOffsets(heldOffsets, 0)
	require.NoError(t, err)
	// A generation change now cancels the old owner promptly. Pause fresh
	// applies before changing authority so they cannot race this inspection.
	recoveryDocker(t, "exec", container, "touch", "/run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
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
	// The held response's HWM is now obsolete, even if its data was committed.
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10002, 0, "after-generation-change", "all", true))
	commit(10002)
	eventResponses = append(eventResponses, appendGenerationEvent(t, available, 3))
	requireReplicaOffsetsEventually(t, available, history, 10002)
	requireGenerationReplicaExcluded(t, available, history, offline, 10002)
	// Pause subsequent fresh attempts so the rejected old response can be
	// inspected before a valid new response repairs the tail.
	recoveryDocker(t, "exec", container, "sh", "-c", "touch /run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history "+hold+".release")
	require.NoError(t, eventually(t, "old leader response rejected", clusterReadyTimeout, func() (bool, string, error) {
		logs := recoveryDocker(t, "logs", container)
		for _, line := range strings.Split(logs, "\n") {
			rejected := strings.Contains(line, `error="replica recovery fence changed"`) || strings.Contains(line, `error="context canceled"`) || strings.Contains(line, `error="context deadline exceeded"`)
			if strings.Contains(line, `protocol=1 topic="indexed-tail-history"`) && strings.Contains(line, fmt.Sprintf(`source="broker-%d-9000"`, leader)) && rejected {
				return true, "original response fenced or canceled before mutation", nil
			}
		}
		return false, "waiting for fenced response", nil
	}))
	response, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 3*time.Second)
	require.NoError(t, err)
	leo, hwm, err := parsePartitionOffsets(response, 0)
	require.NoError(t, err)
	require.EqualValues(t, 10001, leo, "old leader response must not truncate the local tail")
	require.Equal(t, heldLEO, leo)
	require.Equal(t, heldHWM, hwm, "old response must not publish a new HWM")
	require.Equal(t, unchangedLog, generationRecoveryLogDigest(t, container, history), "old response must not rewrite any persisted record")
	requireGenerationReplicaExcluded(t, available, history, offline, 10002)
	t.Log("delayed old-leader response rejected before mutation")
	recoveryDocker(t, "exec", container, "rm", "/run/cursus-e2e-faults/replica-catchup-pause/indexed-tail-history")
	commitsDuringRecovery := 0
	require.NoError(t, eventuallyStable(t, "full ISR while consumer offsets continue", 2*clusterReadyTimeout, rollingRestartISRStabilityWindow, func() (bool, string, error) {
		commit(10002)
		commitsDuringRecovery++
		return fullISRAndZeroUnderReplicated(ctx)
	}))
	require.Greater(t, commitsDuringRecovery, 1)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10002)
	logGenerationRecoveryWork(t, workBefore)
	verifyGenerationEvents(t, ctx.GetBrokerAddrs(), eventResponses)
	require.Equal(t, uint64(10002), fetchCommittedOffset(t, reader, history, group))
	leave, err := reader.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", history, group, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leave, "OK"), leave)
	t.Logf("live offset commits: 10 during offline appends, 1 under held response, %d during recovery", commitsDuringRecovery)
	bytes := recoveryWorkMetric(t, offline, "catchup_rpc", "transfer_bytes") - before
	require.Positive(t, bytes)
	require.Less(t, bytes, float64(8<<20), "recovery transfers the authoritative history with bounded protocol overhead")
	logs := recoveryDocker(t, "logs", container)
	exchanges := 0
	var repair string
	lines := strings.Split(logs, "\n")
	for _, line := range lines[min(previousLines, len(lines)):] {
		if !strings.Contains(line, `protocol=1 topic="indexed-tail-history"`) {
			continue
		}
		if strings.Contains(line, "phase=divergence_repaired") {
			repair = line
		}
		for _, field := range strings.Fields(line) {
			var count int
			if _, err := fmt.Sscanf(field, "exchanges=%d", &count); err == nil {
				exchanges += count
			}
		}
	}
	require.Positive(t, exchanges, "the indexed protocol must be observed")
	// Each of the two generations may need up to 14 binary probes after the
	// sparse challenge. Include the original 24-exchange transfer/retry budget.
	require.LessOrEqual(t, exchanges, 24+2*14)
	// End=1 is absent from the sparse set. Read-only refinement must preserve
	// the committed seed instead of clearing it and reconstructing from zero.
	require.Contains(t, repair, "start=10001 current=1 ")
	t.Logf("observed authenticated repair boundary: %s", repair)
	t.Logf("10000 divergent epoch-zero records repaired; indexed exchanges including failed turns=%d incremental response bytes=%.0f", exchanges, bytes)
	t.Logf("receiver lineage work after startup: read bytes=%.0f hash bytes=%.0f", recoveryWorkMetric(t, offline, "lineage", "read_bytes")-readBefore, recoveryWorkMetric(t, offline, "lineage", "canonical_hash_bytes")-hashBefore)
	epoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	want := readRecoveryHistory(t, history, leader, newLeader, epoch, 10002)
	require.Len(t, want, 10002)
	require.Equal(t, "committed-prefix", want[0].Payload)
	require.Equal(t, committedSeed, want[:1], "every persisted field in the original committed prefix must survive")
	for i := 1; i < 10001; i++ {
		require.Equal(t, fmt.Sprintf("authoritative-%d", i-1), want[i].Payload)
	}
	require.Equal(t, "after-generation-change", want[10001].Payload)
	for node := 1; node <= 3; node++ {
		require.Equal(t, want, readRecoveryHistory(t, history, node, newLeader, epoch, 10002))
	}
	// Exercise a second recovery with an abrupt exit after the durable truncate,
	// before producer/HWM reconstruction and incremental replay can complete.
	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-truncate-crash && touch /run/cursus-e2e-faults/replica-truncate-crash/indexed-tail-history-partition_0")
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	recoveryDocker(t, "run", "--rm", "--volumes-from", container, "-e", "CURSUS_E2E_FAULTS=1", "-e", "CURSUS_E2E_TAIL_START=10002", "--entrypoint", "/app/recovery-fixture", imageID)
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10003, 0, "after-truncate-crash", "all", true))
	recoveryDocker(t, "start", container)
	require.NoError(t, eventually(t, "process exits after durable truncation", clusterReadyTimeout, func() (bool, string, error) {
		state := recoveryDocker(t, "inspect", "--format={{.State.Running}}:{{.State.ExitCode}}", container)
		return state == "false:87", state, nil
	}))
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "interrupted truncation and incremental replay")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10003)
	got := readRecoveryHistory(t, history, offline, newLeader, epoch, 10003)
	require.Len(t, got, 10003)
	require.Equal(t, want, got[:10002])
	require.Equal(t, "after-truncate-crash", got[10002].Payload)

	recoveryDocker(t, "exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/replica-catchup-crash && touch /run/cursus-e2e-faults/replica-catchup-crash/indexed-tail-history-0")
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10004, 0, "after-incremental-crash", "all", true))
	recoveryDocker(t, "start", container)
	require.NoError(t, eventually(t, "process exits after incremental append before HWM publication", 2*time.Minute, func() (bool, string, error) {
		state := recoveryDocker(t, "inspect", "--format={{.State.Running}}:{{.State.ExitCode}}", container)
		return state == "false:88", state, nil
	}))
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "interrupted incremental append")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10004)
	for node := 1; node <= 3; node++ {
		records := readRecoveryHistory(t, history, node, newLeader, epoch, 10004)
		require.Len(t, records, 10004)
		require.Equal(t, got, records[:10003])
		require.Equal(t, "after-incremental-crash", records[10003].Payload)
	}
	beforeRestart := readRecoveryHistory(t, history, offline, newLeader, epoch, 10004)
	restartGenerationRecoveryCluster(t, ctx, actions)
	epoch = requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	description, err = client.SendCommand("", "DESCRIBE topic="+history, 5*time.Second)
	require.NoError(t, err)
	newLeader, err = leaderNodeFromDescribe(description, 3)
	require.NoError(t, err)
	for node := 1; node <= 3; node++ {
		require.Equal(t, beforeRestart, readRecoveryHistory(t, history, node, newLeader, epoch, 10004))
	}
	require.Equal(t, uint64(10002), fetchCommittedOffset(t, client, history, group))
	// Reconstructed producer state must suppress the last accepted sequence.
	require.NoError(t, client.PublishIdempotentToPartition(history, "seed", 0, 10004, 0, "after-incremental-crash", "all", true))
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 10004)
	t.Log("whole-cluster restart preserved every raw record, consumer offset and producer deduplication")
	verifyGenerationEvents(t, ctx.GetBrokerAddrs(), eventResponses)
	eventResponses = append(eventResponses, appendGenerationEvent(t, ctx.GetBrokerAddrs(), 4))
	verifyGenerationEvents(t, ctx.GetBrokerAddrs(), eventResponses)
}
