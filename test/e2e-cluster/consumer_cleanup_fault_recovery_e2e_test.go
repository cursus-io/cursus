package e2e_cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

const cleanupRecoveryTopic = "cleanup-fault-orders"
const cleanupRecoveryGroup = "cleanup-fault-workers"
const cleanupRetainedEvents = "cleanup-retained-events"
const cleanupRetainedMessages = "cleanup-retained-messages"

// Exercise cleanup with actual coordinator movement and actual snapshot restores.
// Every fault boundary checks records and both event-ID and producer-sequence
// deduplication on unrelated topics, including continued fresh writes.
func TestCommittedConsumerCleanupSurvivesFailoverAndSnapshot(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_RECOVERY") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_RECOVERY=1 to run cleanup fault recovery")
	}
	ctx := givenCleanupCluster(t, composeFile, snapshotComposeFile, "docker-compose.consumer-cleanup.yml", "docker-compose.consumer-cleanup-recovery.yml").
		WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	for _, topicName := range []string{cleanupRecoveryTopic, cleanupRetainedMessages} {
		sendClusterTopicCommand(t, addrs, "CREATE topic="+topicName+" partitions=1 replication_factor=3 idempotent=true")
	}
	sendClusterTopicCommand(t, addrs, "CREATE topic="+cleanupRetainedEvents+" partitions=1 replication_factor=3 aggregate_replay=true")
	sendClusterTopicCommand(t, addrs, "CREATE topic=cleanup-concurrent partitions=1 replication_factor=3 idempotent=true")
	// CREATE acknowledges committed intent before every follower has applied it.
	// Seed data only after all local replicas expose the initial committed range.
	for _, topicName := range []string{cleanupRecoveryTopic, cleanupRetainedMessages, cleanupRetainedEvents, "cleanup-concurrent"} {
		requireClusterDefinitionEventually(t, addrs, topicName, map[string]string{"revision": "1"})
		requireReplicaOffsetsEventually(t, addrs, topicName, 0)
	}
	requireClusterStatusMaterialized(t, addrs)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "initial truncate fixture")
	seedLargeCleanupHistory(t, ctx)
	proof := &cleanupRetainedProof{}
	admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, MaxRetries: 3, RequestTimeoutMS: 15000})
	require.NoError(t, err)
	definition, err := admin.UpdateTopic(cleanupRecoveryTopic, sdk.TopicDefinitionPatch{})
	require.NoError(t, err)
	require.NotEmpty(t, definition.InstanceID)
	truncateOptions := sdk.TruncateTopicOptions{ExpectedRevision: definition.Revision, ExpectedInstance: definition.InstanceID}
	publishTruncateTarget(t, addrs, "old-target-record")
	proof.advance(t, addrs)
	client, generation, member := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
	defer client.Close()
	stopSetupHeartbeat := keepCleanupSetupMemberAlive(t, addrs, generation, member)
	commitGroupOffset(t, client, cleanupRecoveryTopic, cleanupRecoveryGroup, 9, generation, member)
	oldID := findGroupCoordinator(t, 1, cleanupRecoveryGroup)
	oldOwner := coordinatorNode(t, oldID)
	for _, node := range survivorNodes(oldOwner) {
		heartbeatGroup(t, client, cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
		cleanupBrokerCommand(t, "restart", node)
		require.NoError(t, actions.waitForNodeHealth(node, clusterHealthCheckAddrs(3)[node-1]))
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, "live cleanup group recovered on follower")
	}
	stopSetupHeartbeat()
	leaveCleanupGroup(t, client, generation, member)
	cleanupBrokerCommand(t, "stop", oldOwner)
	available := availableBrokerAddrs(addrs, oldOwner)
	var newID string
	require.NoError(t, eventually(t, "replacement cleanup coordinator", 2*clusterReadyTimeout, func() (bool, string, error) {
		newID = ""
		for _, node := range survivorNodes(oldOwner) {
			id, err := findGroupCoordinatorFrom(node, cleanupRecoveryGroup)
			if err != nil || id == oldID || (newID != "" && newID != id) {
				return false, fmt.Sprintf("node=%d coordinator=%s old=%s err=%v", node, id, oldID, err), nil
			}
			newID = id
		}
		return newID != "", newID, nil
	}))
	t.Logf("coordinator failover proven: old=%s new=%s", oldID, newID)
	recovered := e2e.NewBrokerClient(available)
	defer recovered.Close()
	require.Equal(t, uint64(9), fetchCommittedOffset(t, recovered, cleanupRecoveryTopic, cleanupRecoveryGroup))
	for _, topicName := range []string{cleanupRetainedEvents, cleanupRetainedMessages, "cleanup-concurrent"} {
		if topicName == "cleanup-concurrent" {
			requireFailoverISRReady(t, available, topicName, oldOwner, 0)
			continue
		}
		requireFailoverISRReady(t, available, topicName, oldOwner, uint64(proof.count))
	}
	stopTraffic := startTruncateConcurrentTraffic(t, available)
	finishDelayedRegistration := holdCleanupRegistration(t, available[0])
	setCleanupReadFault(t, survivorNodes(oldOwner), true)
	proxy, dropped := dropTruncateReplyProxy(t, available[0])
	lostReplyAdmin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: append([]string{proxy}, available...), MaxRetries: 4, RequestTimeoutMS: 15000})
	require.NoError(t, err)
	truncateResult, err := lostReplyAdmin.TruncateTopic(cleanupRecoveryTopic, truncateOptions)
	require.NoError(t, err)
	require.True(t, truncateResult.Replayed)
	require.True(t, truncateResult.CleanupPending, "committed intent must not claim completed materialization during metadata read failure")
	require.NoError(t, <-dropped, "proxy must receive and discard the committed response")
	trafficCount := stopTraffic()
	verifyTruncateConcurrentTraffic(t, available, trafficCount)
	requireCleanupReadFaultObserved(t, available, survivorNodes(oldOwner))
	setCleanupReadFault(t, survivorNodes(oldOwner), false)
	requireClusterDefinitionEventually(t, available, cleanupRecoveryTopic, map[string]string{"revision": "2", "lifecycle_epoch": "2"})
	requireCleanupSurvivorsMaterialized(t, available)
	requireReplicaOffsetsEventually(t, available, cleanupRecoveryTopic, 0)
	publishTruncateTarget(t, available, "fresh-target-record")
	proof.verify(t, available)
	requireCleanupGroupAbsent(t, available)
	logCleanupRecoveryWork(t, survivorNodes(oldOwner), "after failover cleanup")

	cleanupBrokerCommand(t, "start", oldOwner)
	require.NoError(t, actions.waitForNodeHealth(oldOwner, clusterHealthCheckAddrs(3)[oldOwner-1]))
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "old coordinator rejoined after cleanup")
	requireClusterStatusMaterialized(t, addrs)
	requireCleanupGroupAbsent(t, addrs)
	proof.advance(t, addrs)
	logCleanupRecoveryWork(t, []int{1, 2, 3}, "old coordinator recovered")
	verifyTruncateReceiptAfterRaftLeaderLoss(t, ctx, actions, truncateOptions)
	restartCleanupFromSnapshots(t, ctx, actions, "truncate")
	requireCleanupGroupAbsent(t, addrs)
	requireClusterDefinitionEventually(t, addrs, cleanupRecoveryTopic, map[string]string{"revision": "2", "lifecycle_epoch": "2"})
	requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 1)
	replayed, err := admin.TruncateTopic(cleanupRecoveryTopic, truncateOptions)
	require.NoError(t, err)
	require.True(t, replayed.Replayed, "receipt survives actual snapshot restore")
	pageCtx, pageCancel := context.WithTimeout(context.Background(), 30*time.Second)
	targetPage, err := admin.BrowseMessages(pageCtx, sdk.BrowseRequest{Topic: cleanupRecoveryTopic, Partition: 0, MaxRecords: 32, MaxBytes: 65536})
	pageCancel()
	require.NoError(t, err)
	require.Len(t, targetPage.Messages, 1)
	require.Equal(t, "fresh-target-record", targetPage.Messages[0].Payload, "old data stays removed and a same-sequence fresh record survives retries and restore")
	proof.advance(t, addrs)

	finishDelayedRegistration()
	recreated, nextGeneration, nextMember := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
	defer recreated.Close()
	require.Equal(t, uint64(0), fetchCommittedOffset(t, recreated, cleanupRecoveryTopic, cleanupRecoveryGroup), "old offset must not resurrect")
	heartbeatGroup(t, recreated, cleanupRecoveryTopic, cleanupRecoveryGroup, nextGeneration, nextMember)
	commitGroupOffset(t, recreated, cleanupRecoveryTopic, cleanupRecoveryGroup, 1, nextGeneration, nextMember)
	replayed, err = admin.TruncateTopic(cleanupRecoveryTopic, truncateOptions)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, recreated, cleanupRecoveryTopic, cleanupRecoveryGroup), "retry must preserve new active group offsets")
	leaveCleanupGroup(t, recreated, nextGeneration, nextMember)
	sendClusterTopicCommand(t, addrs, "DELETE topic="+cleanupRecoveryTopic)
	requireClusterStatusMaterialized(t, addrs)
	requireCleanupGroupAbsent(t, addrs)
	// The full-ISR probe must describe a surviving topic after target deletion.
	ctx.WithTopic(cleanupRetainedEvents)
	restartCleanupFromSnapshots(t, ctx, actions, "delete")
	for node := 1; node <= 3; node++ {
		require.NoError(t, actions.WaitForLocalTopicDefinition(node, LocalTopicDefinition{Topic: cleanupRecoveryTopic}, false))
	}
	requireCleanupGroupAbsent(t, addrs)
	proof.advance(t, addrs)
	verifyTruncateConcurrentTraffic(t, addrs, trafficCount)
}

func cleanupBrokerCommand(t *testing.T, operation string, nodes ...int) string {
	t.Helper()
	args := []string{operation}
	for _, node := range nodes {
		require.Contains(t, []int{1, 2, 3}, node)
		args = append(args, cleanupContainerName(node))
	}
	commandCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "docker", args...).CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

func leaveCleanupGroup(t *testing.T, client *e2e.BrokerClient, generation int, member string) {
	t.Helper()
	response, err := client.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", cleanupRecoveryTopic, cleanupRecoveryGroup, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
}

func requireCleanupGroupAbsent(t *testing.T, addrs []string) {
	t.Helper()
	for _, addr := range addrs {
		client := e2e.NewBrokerClient([]string{addr})
		_, err := client.GetConsumerGroupStatus(cleanupRecoveryGroup)
		client.Close()
		var brokerErr *wire.BrokerError
		require.ErrorAs(t, err, &brokerErr, "seed=%s", addr)
		require.Equal(t, "group_status_failed", brokerErr.Code)
		require.Equal(t, "group '"+cleanupRecoveryGroup+"' not found", brokerErr.Fields["reason"])
	}
}

// The deliberately stopped replica makes overall health false. Still require
// local materialization, usable leaders and min-ISR on every surviving broker.
func requireCleanupSurvivorsMaterialized(t *testing.T, addrs []string) {
	t.Helper()
	require.NoError(t, eventually(t, "surviving cleanup replicas materialized", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
			client.Close()
			if err != nil || !strings.HasPrefix(response, "OK cluster=") {
				return false, fmt.Sprintf("%s response=%q err=%v", addr, response, err), nil
			}
			var status struct {
				ActiveBrokers                 int `json:"active_brokers"`
				Leaderless                    int `json:"leaderless_partitions"`
				MinISRUnsatisfied             int `json:"min_isr_unsatisfied_partitions"`
				RecoveryPending               int `json:"recovery_pending_partitions"`
				TopicMaterializationPending   int `json:"topic_materialization_pending"`
				ReplicaMaterializationPending int `json:"replica_materialization_pending"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
				return false, response, err
			}
			if status.ActiveBrokers != len(addrs) || status.Leaderless != 0 || status.MinISRUnsatisfied != 0 || status.RecoveryPending != 0 || status.TopicMaterializationPending != 0 || status.ReplicaMaterializationPending != 0 {
				return false, fmt.Sprintf("%s status=%+v", addr, status), nil
			}
		}
		return true, "all surviving brokers materialized with usable quorum", nil
	}))
}

type cleanupRetainedProof struct {
	count           int
	appendResponses []string
}

func cleanupEventCommand(version int) string {
	return fmt.Sprintf("APPEND_STREAM topic=%s key=retained-order version=%d event_id=retained-event-%d producerId=retained-event-writer seqNum=%d event_type=Retained schema_version=1 message=event-%d", cleanupRetainedEvents, version, version, version, version)
}

func (proof *cleanupRetainedProof) advance(t *testing.T, addrs []string) {
	t.Helper()
	if proof.count > 0 {
		proof.verify(t, addrs)
	}
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	version := proof.count + 1
	response, err := client.SendCommand("", cleanupEventCommand(version), 15*time.Second)
	require.NoError(t, err)
	require.Contains(t, response, fmt.Sprintf("OK version=%d offset=%d partition=0", version, version-1))
	proof.appendResponses = append(proof.appendResponses, response)
	require.NoError(t, client.PublishIdempotentToPartition(cleanupRetainedMessages, "retained-producer", 0, uint64(version), 1, fmt.Sprintf("message-%d", version), "all", true))
	proof.count = version
	proof.verify(t, addrs)
}

func (proof *cleanupRetainedProof) verify(t *testing.T, addrs []string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	for version := 1; version <= proof.count; version++ {
		response, err := client.SendCommand("", cleanupEventCommand(version), 15*time.Second)
		require.NoError(t, err, "event retry version=%d", version)
		require.Equal(t, proof.appendResponses[version-1], response)
	}
	// The producer contract retains its last accepted sequence. Retry that
	// sequence, not an older one that is legitimately outside its dedupe window.
	require.NoError(t, client.PublishIdempotentToPartition(cleanupRetainedMessages, "retained-producer", 0, uint64(proof.count), 1, fmt.Sprintf("message-%d", proof.count), "all", true))
	for _, addr := range addrs {
		admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: []string{addr}, RequestTimeoutMS: 3000, MaxRetries: 5, RetryBackoffMS: 100})
		require.NoError(t, err)
		requestCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		history, err := admin.ReadStreamHistory(requestCtx, sdk.HistoryRequest{Topic: cleanupRetainedEvents, Key: "retained-order", FromVersion: 1, MaxRecords: 32, MaxBytes: 65536})
		cancel()
		require.NoError(t, err, "history seed=%s", addr)
		require.Equal(t, sdk.HistoryComplete, history.Completeness)
		require.False(t, history.HasMore)
		require.Len(t, history.Events, proof.count)
		for i, event := range history.Events {
			require.Equal(t, uint64(i+1), event.Version)
			require.Equal(t, fmt.Sprintf("event-%d", i+1), event.Payload)
		}
		requestCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		browse, err := admin.BrowseMessages(requestCtx, sdk.BrowseRequest{Topic: cleanupRetainedMessages, Partition: 0, MaxRecords: 32, MaxBytes: 65536})
		cancel()
		require.NoError(t, err, "browse seed=%s", addr)
		require.Len(t, browse.Messages, proof.count)
		for i, message := range browse.Messages {
			require.Equal(t, fmt.Sprintf("message-%d", i+1), message.Payload)
		}
	}
	for _, topicName := range []string{cleanupRetainedEvents, cleanupRetainedMessages} {
		requireReplicaOffsetsEventually(t, addrs, topicName, uint64(proof.count))
	}
	t.Logf("unrelated history and dedup preserved: events=%d messages=%d, no duplicate offsets", proof.count, proof.count)
}

func restartCleanupFromSnapshots(t *testing.T, ctx *ClusterTestContext, actions *ClusterActions, phase string) {
	t.Helper()
	targets := captureLocalAppliedIndices(t, actions)
	// A bounded number of metadata commands advances the normal snapshot
	// threshold; never remove logs or substitute a fabricated snapshot.
	for filler := 0; filler < maxSnapshotFillerTopics; filler++ {
		ready := true
		for node := 1; node <= 3; node++ {
			status, err := actions.LocalClusterStatus(node)
			require.NoError(t, err)
			ready = ready && status.RaftLastSnapshotIndex >= targets[node]
		}
		if ready {
			break
		}
		sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), fmt.Sprintf("CREATE topic=cleanup-%s-snapshot-%02d partitions=1 replication_factor=3", phase, filler))
	}
	for node := 1; node <= 3; node++ {
		status, err := actions.WaitForLocalSnapshot(node, targets[node])
		require.NoError(t, err)
		targets[node] = status.RaftLastSnapshotIndex
		output, err := exec.Command("docker", "exec", cleanupContainerName(node), "sh", "-c", "wc -c /app/cluster-logs/raft/snapshots/*/state.bin").CombinedOutput()
		require.NoError(t, err, string(output))
		t.Logf("%s snapshot node=%d index=%d stored bytes: %s", phase, node, status.RaftLastSnapshotIndex, strings.TrimSpace(string(output)))
	}
	cleanupBrokerCommand(t, "stop", 1, 2, 3)
	since := time.Now().UTC().Format(time.RFC3339Nano)
	cleanupBrokerCommand(t, "start", 1, 2, 3)
	for node := 1; node <= 3; node++ {
		require.NoError(t, actions.waitForNodeHealth(node, clusterHealthCheckAddrs(3)[node-1]))
		_, err := actions.WaitForLocalSnapshot(node, targets[node])
		require.NoError(t, err)
		logs, err := exec.Command("docker", "logs", "--since", since, cleanupContainerName(node)).CombinedOutput()
		require.NoError(t, err, string(logs))
		require.Contains(t, string(logs), "FSM restore completed", "node=%d must actually restore a snapshot", node)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, phase+" snapshot restart")
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	logCleanupRecoveryWork(t, []int{1, 2, 3}, phase+" snapshot restart")
}

func logCleanupRecoveryWork(t *testing.T, nodes []int, phase string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	for _, node := range nodes {
		response, err := client.Get(fmt.Sprintf("http://localhost:%d/metrics", baseExporterPort+clusterPortShift()+node))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "consumer_cleanup_replay_") || strings.HasPrefix(line, "cursus_recovery_work_total{") || strings.HasPrefix(line, "go_memstats_heap_alloc_bytes ") || strings.HasPrefix(line, "process_cpu_seconds_total ") {
				t.Logf("%s node=%d %s", phase, node, line)
			}
		}
		err = scanner.Err()
		_ = response.Body.Close()
		require.NoError(t, err)
		if os.Getenv("RUN_E2E_CLEANUP_LARGE") == "1" {
			output, readErr := exec.Command("docker", "exec", cleanupContainerName(node), "cat", "/proc/1/io").CombinedOutput()
			require.NoError(t, readErr, string(output))
			t.Logf("%s node=%d cumulative_process_io=%s", phase, node, strings.ReplaceAll(string(output), "\n", ", "))
		}
	}
}
