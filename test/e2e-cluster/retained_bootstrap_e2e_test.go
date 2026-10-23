package e2e_cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestRetainedBootstrapRestoresOfflineReplicaWithoutDeletingData(t *testing.T) {
	verifyRetainedBootstrap(t, false)
}

func TestRetainedBootstrapResumesAfterProcessCrashDuringInstall(t *testing.T) {
	verifyRetainedBootstrap(t, true)
}

func verifyRetainedBootstrap(t *testing.T, crash bool) {
	t.Helper()
	if os.Getenv("RUN_E2E_RETAINED_BOOTSTRAP") != "1" {
		t.Skip("set RUN_E2E_RETAINED_BOOTSTRAP=1 for retained-range bootstrap validation")
	}
	files := []string{composeFile, "docker-compose.recovery.yml"}
	if crash {
		files = []string{composeFile, "docker-compose.faults.yml", "docker-compose.recovery.yml"}
	}
	ctx := givenClusterRestart(t, "", files...).WithTopic("retained-bootstrap").WithPartitions(1).WithAcks("all")
	actions := ctx.WhenCluster().StartCluster()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	_, err := client.SendCommand("", "CREATE topic="+ctx.GetTopic()+" partitions=1 replication_factor=3 idempotent=true retention_bytes=65536", 5*time.Second)
	require.NoError(t, err)
	actions.WaitForTopicMetadata()
	const unrelated, group = "bootstrap-unrelated", "bootstrap-unrelated-reader"
	require.NoError(t, client.CreateTopic(unrelated, 1, true))
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), unrelated, 0)
	require.NoError(t, client.PublishIdempotent(unrelated, "unrelated-producer", 1, 0, "preserved", "all", true))
	reader, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), unrelated, group)
	defer reader.Close()
	commitGroupOffset(t, reader, unrelated, group, 1, generation, member)
	payload := strings.Repeat("r", 4096)
	publish := func(sequence uint64) {
		t.Helper()
		require.NoError(t, client.PublishIdempotentToPartition(ctx.GetTopic(), "retained-producer", 0, sequence, 0, fmt.Sprintf("%d:%s", sequence, payload), "all", true))
	}
	for sequence := uint64(1); sequence <= 8; sequence++ {
		publish(sequence)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "retained bootstrap seed")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 8)
	// Choose a follower so the surviving source keeps the original leader epoch.
	describe, err := client.SendCommand("", "DESCRIBE topic="+ctx.GetTopic(), 5*time.Second)
	require.NoError(t, err)
	leader, err := leaderNodeFromDescribe(describe, 3)
	require.NoError(t, err)
	offline := 1
	if leader == offline {
		offline = 2
	}
	container := fmt.Sprintf("%sbroker-%d", os.Getenv("CURSUS_E2E_CONTAINER_PREFIX"), offline)
	docker := func(args ...string) string {
		t.Helper()
		commandCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		output, err := exec.CommandContext(commandCtx, "docker", args...).CombinedOutput()
		require.NoError(t, err, "%s", output)
		return strings.TrimSpace(string(output))
	}
	if crash {
		docker("exec", container, "sh", "-c", "mkdir -p /run/cursus-e2e-faults/bootstrap-install-crash && touch /run/cursus-e2e-faults/bootstrap-install-crash/retained-bootstrap-partition_0")
	}
	actions.StopBroker(offline)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	for sequence := uint64(9); sequence <= 128; sequence++ {
		publish(sequence)
	}
	source := e2e.NewBrokerClient([]string{fmt.Sprintf("localhost:%d", brokerPort(leader))})
	defer source.Close()
	var retainedStart uint64
	require.NoError(t, eventually(t, "retention passes offline replica end", clusterReadyTimeout, func() (bool, string, error) {
		response, err := source.SendCommand("", "LIST_OFFSETS topic="+ctx.GetTopic()+" partition=0", 3*time.Second)
		if err != nil {
			return false, "source offsets unavailable", nil
		}
		for _, field := range strings.Split(response, ":") {
			if strings.HasPrefix(field, "earliest=") {
				retainedStart, err = strconv.ParseUint(strings.TrimPrefix(field, "earliest="), 10, 64)
				break
			}
		}
		return retainedStart > 8, response, err
	}))
	const transactionID = "retained-bootstrap-transaction"
	producer, err := client.InitTransactionProducer(transactionID)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction(transactionID, producer))
	require.NoError(t, client.TransactionalPublish(transactionID, ctx.GetTopic(), 0, producer, 1, "retained-transaction"))
	require.NoError(t, client.EndTransaction(transactionID, producer, "commit"))
	t.Logf("offline replica end=8 source retained start=%d committed end=130", retainedStart)
	if crash {
		docker("start", container)
		require.NoError(t, eventually(t, "process exits during bootstrap replacement", clusterReadyTimeout, func() (bool, string, error) {
			status := docker("inspect", "--format={{.State.Running}}:{{.State.ExitCode}}", container)
			return status == "false:86", status, nil
		}))
		// Copy the pending manifest for evidence; the broker's volume is intact.
		docker("cp", container+":/app/cluster-logs/retained-bootstrap/partition_0.bootstrap.pending", filepath.Join(t.TempDir(), "pending.json"))
	}
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "retained bootstrap installation")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 130)
	transfer := recoveryWorkMetric(t, offline, "replica_rpc", "transfer_bytes")
	writes := recoveryWorkMetric(t, offline, "chunk_receive", "write_bytes")
	hashes := recoveryWorkMetric(t, offline, "checksum", "hash_bytes")
	if !crash {
		require.Positive(t, transfer, "the replica must receive a bootstrap, not only ordinary catch-up")
		require.Positive(t, writes)
	}
	require.Positive(t, hashes)
	t.Logf("bootstrap received payload bytes=%.0f staging write bytes=%.0f checksum read bytes=%.0f", transfer, writes, hashes)
	publish(129)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 131)
	if crash {
		const indexPath = "/app/cluster-logs/retained-bootstrap/partition_0.lineage"
		imageID := docker("inspect", "--format={{.Image}}", container)
		for _, damage := range []string{"missing", "damaged"} {
			actions.StopBroker(offline)
			// Rebuild is driven by a fresh recovery proof. A fast restart can
			// retain the old ISR membership and legitimately skip that work.
			waitForBrokerEvictedFromISR(t, ctx, offline)
			command := "test -s " + indexPath + " && rm " + indexPath
			if damage == "damaged" {
				command = "test -s " + indexPath + " && printf invalid > " + indexPath
			}
			// The helper mounts only this stopped test broker's volume. Raw logs,
			// watermarks and transaction/consumer state remain untouched.
			docker("run", "--rm", "--volumes-from", container, "--entrypoint", "sh", imageID, "-c", command)
			actions.startBrokerWithin(offline, 2*time.Minute)
			waitForStableFullISRAndZeroUnderReplicated(t, ctx, damage+" lineage rebuild")
			requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 131)
			copyPath := filepath.Join(t.TempDir(), "rebuilt.lineage")
			docker("cp", container+":"+indexPath, copyPath)
			data, err := os.ReadFile(copyPath)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(string(data), "CLIN0001"))
			t.Logf("%s lineage rebuilt after restart; sidecar bytes=%d", damage, len(data))
		}
	}
	actions.StopBroker(offline)
	actions.startBrokerWithin(offline, 2*time.Minute)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "second restart after retained bootstrap")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 131)
	// The original connection can exceed the server's idle timeout during the
	// crash/rebuild sequence. Election has an ambiguous-delivery contract, so use
	// a fresh connection instead of retrying a possibly delivered command.
	control := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer control.Close()
	_, err = control.SendCommand("", fmt.Sprintf("ELECT_LEADER topic=%s partition=0 broker=broker-%d-9000", ctx.GetTopic(), offline), 5*time.Second)
	require.NoError(t, err)
	const retainedGroup = "retained-bootstrap-reader"
	retainedReader, retainedGeneration, retainedMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), retainedGroup)
	defer retainedReader.Close()
	recovered := e2e.NewBrokerClient([]string{fmt.Sprintf("localhost:%d", brokerPort(offline))})
	defer recovered.Close()
	messages, _, err := recovered.ConsumeMessagesAtOffsetWithOffsets(ctx.GetTopic(), 0, 128, retainedGroup, retainedMember, retainedGeneration, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"retained-transaction", "129:" + payload}, messages)
	require.NoError(t, control.EndTransaction(transactionID, producer, "commit"))
	offset, err := control.FetchCommittedOffset(unrelated, 0, group)
	require.NoError(t, err)
	require.Equal(t, uint64(1), offset)
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), unrelated, 1)
}

func recoveryWorkMetric(t *testing.T, node int, stage, unit string) float64 {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://localhost:%d/metrics", 9100+node))
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	require.NoError(t, err)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "cursus_recovery_work_total{") && strings.Contains(line, `stage="`+stage+`"`) && strings.Contains(line, `unit="`+unit+`"`) {
			fields := strings.Fields(line)
			value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
			require.NoError(t, err)
			return value
		}
	}
	return 0
}
