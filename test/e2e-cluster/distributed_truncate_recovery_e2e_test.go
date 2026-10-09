package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestDistributedTruncateRecoveryPreservesConsumerOffsetReplication(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run distributed truncate recovery validation")
	}

	const businessTopic = "truncate-recovery-business"
	lifecycleTopics := []string{"truncate-recovery-state", "truncate-recovery-commands", "truncate-recovery-history"}
	ctx := GivenFaultClusterRestart(t).WithClusterSize(3).WithTopic(businessTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	for _, topicName := range append([]string{businessTopic}, lifecycleTopics...) {
		sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
			"CREATE topic="+topicName+" partitions=1 replication_factor=3",
		)
		requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
			"revision": "1", "lifecycle_epoch": "1", "partitions": "1", "replication_factor": "3",
		})
	}

	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	for _, topicName := range append([]string{businessTopic}, lifecycleTopics...) {
		require.NoError(t, client.PublishIdempotentToPartition(topicName, "truncate-recovery-producer", 0, 1, 0, "before-truncate", "all", true))
	}
	client.Close()
	const group = "truncate-recovery-during-restart"
	seedGroupClient, seedGeneration, seedMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), businessTopic, group)
	commitResponse, err := seedGroupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=1 generation=%d member=%s",
		businessTopic, group, seedGeneration, seedMember,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitResponse, "OK"), commitResponse)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, seedGroupClient, businessTopic, group))
	leaveResponse, err := seedGroupClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, group, seedMember, seedGeneration,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
	seedGroupClient.Close()

	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "TRUNCATE topic="+lifecycleTopics[0]+" expected_revision=1")
	follower := waitForRaftFollower(t, actions)
	actions.StopBroker(follower)
	available := availableBrokerAddrs(ctx.GetBrokerAddrs(), follower)
	requireFailoverISRReady(t, available, businessTopic, follower, 1)

	truncateWithAmbiguousResponse(t, available, lifecycleTopics[1])
	sendClusterTopicCommand(t, available, "TRUNCATE topic="+lifecycleTopics[2]+" expected_revision=1")

	actions.StartBroker(follower)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "distributed truncate recovery")
	for _, topicName := range lifecycleTopics {
		requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
			"revision": "2", "lifecycle_epoch": "2",
		})
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), topicName, 0)
	}

	gapClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	require.NoError(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 2, 0, "before-gap", "all", true))
	require.NoError(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 3, 0, "same-epoch-followup", "all", true))
	gapClient.Close()
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), businessTopic, 3)

	groupClient, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), businessTopic, group)
	heartbeatGroup(t, groupClient, businessTopic, group, generation, member)
	offsetPartition, offsetPartitionCount := consumerOffsetPartition(t, ctx.GetBrokerAddrs(), group)
	internalLEO := requireReplicaPartitionOffsetsConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition)
	leaderEpoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition, false, 3, 0)
	for node := 1; node <= 3; node++ {
		setReplicaAppendSkipFault(t, node, config.ConsumerOffsetsTopicName, offsetPartition, internalLEO)
		setReplicaCatchupPauseFault(t, node, config.ConsumerOffsetsTopicName, true)
		defer setReplicaCatchupPauseFaultBestEffort(node, config.ConsumerOffsetsTopicName, false)
	}
	commitGroupOffset(t, groupClient, businessTopic, group, 2, generation, member)
	heartbeatGroup(t, groupClient, businessTopic, group, generation, member)
	commitResponse, err = groupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=3 generation=%d member=%s",
		businessTopic, group, generation, member,
	), 15*time.Second)
	require.True(t, err != nil || strings.HasPrefix(commitResponse, "ERROR"), "same-epoch append unexpectedly passed the internal replica gap: response=%q err=%v", commitResponse, err)
	recoveryNode := requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition, leaderEpoch)
	actions.StopBroker(recoveryNode)
	restartBrokerDuringRecovery(t, recoveryNode)
	requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition, leaderEpoch)
	commitResponse, err = groupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=3 generation=%d member=%s",
		businessTopic, group, generation, member,
	), 15*time.Second)
	require.True(t, err != nil || strings.HasPrefix(commitResponse, "ERROR"), "write unexpectedly passed recovery fence: response=%q err=%v", commitResponse, err)
	groupClient.Close()

	unrelatedGroup := consumerGroupOnDifferentOffsetPartition(group, offsetPartition, offsetPartitionCount)
	unrelatedClient, unrelatedGeneration, unrelatedMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), businessTopic, unrelatedGroup)
	heartbeatGroup(t, unrelatedClient, businessTopic, unrelatedGroup, unrelatedGeneration, unrelatedMember)
	commitGroupOffset(t, unrelatedClient, businessTopic, unrelatedGroup, 1, unrelatedGeneration, unrelatedMember)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, unrelatedClient, businessTopic, unrelatedGroup))
	leaveResponse, err = unrelatedClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, unrelatedGroup, unrelatedMember, unrelatedGeneration,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
	unrelatedClient.Close()
	for node := 1; node <= 3; node++ {
		setReplicaCatchupPauseFault(t, node, config.ConsumerOffsetsTopicName, false)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "replica gap recovery")
	requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition, false, 3, leaderEpoch)
	requireReplicaPartitionOffsetsConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition)

	groupClient = resumeRecoveryGroup(t, ctx.GetBrokerAddrs(), businessTopic, group, member, generation)
	defer groupClient.Close()
	heartbeatGroup(t, groupClient, businessTopic, group, generation, member)
	require.Equal(t, uint64(2), fetchCommittedOffset(t, groupClient, businessTopic, group))
	resumed := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), businessTopic, 0, group, member, generation)
	require.NotEmpty(t, resumed, "consumer must resume from the committed offset after internal replica recovery")
	commitGroupOffset(t, groupClient, businessTopic, group, 3, generation, member)
	require.Equal(t, uint64(3), fetchCommittedOffset(t, groupClient, businessTopic, group))
	requireOffsetResponsesConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName)
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	leaveResponse, err = groupClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, group, member, generation,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
	unrelatedClient, unrelatedGeneration, unrelatedMember = joinRecoveryGroup(t, ctx.GetBrokerAddrs(), businessTopic, unrelatedGroup)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, unrelatedClient, businessTopic, unrelatedGroup))
	heartbeatGroup(t, unrelatedClient, businessTopic, unrelatedGroup, unrelatedGeneration, unrelatedMember)
	leaveResponse, err = unrelatedClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, unrelatedGroup, unrelatedMember, unrelatedGeneration,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
	unrelatedClient.Close()
}

func joinRecoveryGroup(t *testing.T, addrs []string, topicName, group string) (*e2e.BrokerClient, int, string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	generation, member, err := client.JoinGroup(topicName, group)
	if err != nil {
		if !strings.Contains(err.Error(), "write succeeded but read failed") {
			client.Close()
			t.Fatalf("join recovery group %s: %v", group, err)
		}
		generation, member = recoverAmbiguousGroupMember(t, client, group, client.GetMemberID())
		client.SetMemberID(member)
	}
	require.NoError(t, eventually(t, "sync recovery group "+group, clusterReadyTimeout, func() (bool, string, error) {
		_, syncErr := client.SyncGroup(topicName, group, generation, member)
		return syncErr == nil, fmt.Sprintf("member=%s generation=%d err=%v", member, generation, syncErr), nil
	}))
	return client, generation, member
}

func recoverAmbiguousGroupMember(t *testing.T, client *e2e.BrokerClient, group, memberPrefix string) (int, string) {
	t.Helper()
	var generation int
	var recoveredMember string
	require.NoError(t, eventually(t, "recover ambiguous group member "+group, clusterReadyTimeout, func() (bool, string, error) {
		status, err := client.GetConsumerGroupStatus(group)
		if err != nil {
			return false, err.Error(), nil
		}
		matches := make([]string, 0, 1)
		for _, member := range status.Members {
			if member.MemberID == memberPrefix || strings.HasPrefix(member.MemberID, memberPrefix+"-") {
				matches = append(matches, member.MemberID)
			}
		}
		if len(matches) != 1 {
			return false, fmt.Sprintf("generation=%d matching_members=%v", status.Generation, matches), nil
		}
		generation, recoveredMember = status.Generation, matches[0]
		return true, fmt.Sprintf("member=%s generation=%d", recoveredMember, generation), nil
	}))
	return generation, recoveredMember
}

func resumeRecoveryGroup(t *testing.T, addrs []string, topicName, group, member string, generation int) *e2e.BrokerClient {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	deadline := time.Now().Add(clusterReadyTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.SendCommand("", fmt.Sprintf(
			"JOIN_GROUP topic=%s group=%s member=%s generation=%d",
			topicName, group, member, generation,
		), 5*time.Second)
		if err == nil && strings.HasPrefix(response, "OK") {
			if _, err = client.SyncGroup(topicName, group, generation, member); err == nil {
				return client
			}
		}
		lastErr = err
		time.Sleep(clusterPollInterval)
	}
	client.Close()
	t.Fatalf("resume and sync recovery group %s: %v", group, lastErr)
	return nil
}

func heartbeatGroup(t *testing.T, client *e2e.BrokerClient, topicName, group string, generation int, member string) {
	t.Helper()
	response, err := client.SendCommand("", fmt.Sprintf(
		"HEARTBEAT topic=%s group=%s member=%s generation=%d",
		topicName, group, member, generation,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
}

func commitGroupOffset(t *testing.T, client *e2e.BrokerClient, topicName, group string, offset uint64, generation int, member string) {
	t.Helper()
	response, err := client.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=%d generation=%d member=%s",
		topicName, group, offset, generation, member,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
}

func consumerOffsetPartition(t *testing.T, addrs []string, group string) (int, int) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	response, err := client.SendCommand("", "METADATA topic="+config.ConsumerOffsetsTopicName, 5*time.Second)
	client.Close()
	require.NoError(t, err)
	partitionCount, err := strconv.Atoi(topicResponseFields(response)["partitions"])
	require.NoError(t, err, response)
	require.Positive(t, partitionCount, response)
	partition := int(util.GenerateID(coordinator.ConsumerMetadataGroupPartitionKey(group)) % uint64(partitionCount))
	return partition, partitionCount
}

func consumerGroupOnDifferentOffsetPartition(base string, excludedPartition, partitionCount int) string {
	for candidate := 1; ; candidate++ {
		group := fmt.Sprintf("%s-unrelated-%d", base, candidate)
		partition := int(util.GenerateID(coordinator.ConsumerMetadataGroupPartitionKey(group)) % uint64(partitionCount))
		if partition != excludedPartition {
			return group
		}
	}
}

func requireReplicaPartitionOffsetsConverge(t *testing.T, addrs []string, topicName string, partition int) uint64 {
	t.Helper()
	var convergedLEO uint64
	require.NoError(t, eventually(t, "replica partition offsets converge for "+topicName, 2*clusterReadyTimeout, func() (bool, string, error) {
		var expectedLEO, expectedHWM uint64
		for index, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", fmt.Sprintf("LIST_OFFSETS topic=%s partition=%d", topicName, partition), 5*time.Second)
			client.Close()
			if err != nil {
				return false, fmt.Sprintf("%s: %v", addr, err), nil
			}
			leo, hwm, err := parsePartitionOffsets(response, partition)
			if err != nil {
				return false, response, nil
			}
			if leo != hwm {
				return false, fmt.Sprintf("%s leo=%d hwm=%d", addr, leo, hwm), nil
			}
			if index == 0 {
				expectedLEO, expectedHWM = leo, hwm
				continue
			}
			if leo != expectedLEO || hwm != expectedHWM {
				return false, fmt.Sprintf("%s leo=%d hwm=%d expected=%d", addr, leo, hwm, expectedLEO), nil
			}
		}
		convergedLEO = expectedLEO
		return true, fmt.Sprintf("partition=%d leo=%d hwm=%d", partition, expectedLEO, expectedHWM), nil
	}))
	return convergedLEO
}

func parsePartitionOffsets(response string, partition int) (uint64, uint64, error) {
	marker := fmt.Sprintf("P%d:", partition)
	start := strings.Index(response, marker)
	if start < 0 {
		return 0, 0, fmt.Errorf("partition %d offsets missing from %q", partition, response)
	}
	entry := response[start:]
	if end := strings.IndexByte(entry, ','); end >= 0 {
		entry = entry[:end]
	}
	var leo, hwm uint64
	foundLEO, foundHWM := false, false
	for _, field := range strings.Split(entry, ":") {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, 0, err
		}
		switch key {
		case "leo":
			leo, foundLEO = parsed, true
		case "hwm":
			hwm, foundHWM = parsed, true
		}
	}
	if !foundLEO || !foundHWM {
		return 0, 0, fmt.Errorf("LEO/HWM missing from %q", response)
	}
	return leo, hwm, nil
}

func setReplicaAppendSkipFault(t *testing.T, node int, topicName string, partition int, offset uint64) {
	t.Helper()
	name := fmt.Sprintf("%s-%d-%d", topicName, partition, offset)
	setReplicaFaultSentinel(t, node, "replica-append-skip", name, true)
}

func setReplicaCatchupPauseFault(t *testing.T, node int, topicName string, enabled bool) {
	t.Helper()
	setReplicaFaultSentinel(t, node, "replica-catchup-pause", topicName, enabled)
}

func setReplicaCatchupPauseFaultBestEffort(node int, topicName string, enabled bool) {
	_ = runReplicaFaultSentinelCommand(node, "replica-catchup-pause", topicName, enabled)
}

func setReplicaFaultSentinel(t *testing.T, node int, operation, name string, enabled bool) {
	t.Helper()
	if err := runReplicaFaultSentinelCommand(node, operation, name, enabled); err != nil {
		t.Fatalf("toggle broker-%d %s fault for %s: %v", node, operation, name, err)
	}
}

func runReplicaFaultSentinelCommand(node int, operation, name string, enabled bool) error {
	if node < 1 || node > 3 || name == "" || strings.ContainsAny(name, "/\\") {
		return fmt.Errorf("invalid replica fault target")
	}
	service := fmt.Sprintf("broker-%d", node)
	directory := filepath.ToSlash(filepath.Join(faultSentinelRoot, operation))
	sentinel := filepath.ToSlash(filepath.Join(directory, name))
	composeFiles := []string{composeFile, faultComposeFile}
	if enabled {
		if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "mkdir", "-p", directory).CombinedOutput(); err != nil {
			return fmt.Errorf("create replica fault directory: %w: %s", err, output)
		}
		if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "touch", sentinel).CombinedOutput(); err != nil {
			return fmt.Errorf("create replica fault sentinel: %w: %s", err, output)
		}
		return nil
	}
	if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "rm", "-f", sentinel).CombinedOutput(); err != nil {
		return fmt.Errorf("remove replica fault sentinel: %w: %s", err, output)
	}
	return nil
}

func requireRecoveryPendingAndNotReady(t *testing.T, addrs []string, topicName string, partitionID, leaderEpoch int) int {
	t.Helper()
	recoveryNode := 0
	require.NoError(t, eventually(t, "replica recovery pending and readiness closed", clusterReadyTimeout, func() (bool, string, error) {
		client := e2e.NewBrokerClient(addrs)
		response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
		client.Close()
		if err != nil || !strings.HasPrefix(response, "OK cluster=") {
			return false, response, err
		}
		var status struct {
			Healthy         bool `json:"healthy"`
			RecoveryPending int  `json:"recovery_pending_partitions"`
			Partitions      []struct {
				Topic            string   `json:"topic"`
				Partition        int      `json:"partition"`
				LeaderEpoch      int      `json:"leader_epoch"`
				RecoveryReplicas []string `json:"recovery_replicas"`
				RecoveryPending  bool     `json:"recovery_pending"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
			return false, response, err
		}
		if status.Healthy || status.RecoveryPending == 0 {
			return false, fmt.Sprintf("healthy=%t recovery_pending=%d", status.Healthy, status.RecoveryPending), nil
		}
		partitionFound := false
		for _, partition := range status.Partitions {
			if partition.Topic != topicName || partition.Partition != partitionID {
				continue
			}
			partitionFound = true
			if partition.LeaderEpoch != leaderEpoch {
				return false, fmt.Sprintf("leader epoch changed: got=%d want=%d", partition.LeaderEpoch, leaderEpoch), nil
			}
			if !partition.RecoveryPending || len(partition.RecoveryReplicas) == 0 {
				return false, fmt.Sprintf("partition recovery_pending=%t replicas=%v", partition.RecoveryPending, partition.RecoveryReplicas), nil
			}
			parts := strings.Split(strings.TrimPrefix(partition.RecoveryReplicas[0], "broker-"), "-")
			if len(parts) == 0 {
				return false, fmt.Sprintf("invalid recovery broker id %q", partition.RecoveryReplicas[0]), nil
			}
			node, err := strconv.Atoi(parts[0])
			if err != nil || node < 1 || node > len(addrs) {
				return false, fmt.Sprintf("invalid recovery broker id %q", partition.RecoveryReplicas[0]), nil
			}
			recoveryNode = node
		}
		if !partitionFound {
			return false, "recovery partition not found", nil
		}
		for node := 1; node <= 3; node++ {
			code, body, err := readReadiness(node)
			if err != nil {
				return false, fmt.Sprintf("broker-%d readiness error: %v", node, err), nil
			}
			if code == 200 {
				return false, fmt.Sprintf("broker-%d unexpectedly ready: %s", node, body), nil
			}
		}
		return true, fmt.Sprintf("healthy=false recovery_pending=%d", status.RecoveryPending), nil
	}))
	return recoveryNode
}

func restartBrokerDuringRecovery(t *testing.T, node int) {
	t.Helper()
	service := fmt.Sprintf("broker-%d", node)
	if output, err := runClusterCompose([]string{composeFile, faultComposeFile}, "start", service).CombinedOutput(); err != nil {
		t.Fatalf("restart %s during replica recovery: %v: %s", service, err, output)
	}
}

func requirePartitionRecoveryState(t *testing.T, addrs []string, topicName string, partitionID int, pending bool, expectedISR int, expectedEpoch int) int {
	t.Helper()
	observedEpoch := 0
	require.NoError(t, eventually(t, "partition recovery state for "+topicName, clusterReadyTimeout, func() (bool, string, error) {
		client := e2e.NewBrokerClient(addrs)
		response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
		client.Close()
		if err != nil || !strings.HasPrefix(response, "OK cluster=") {
			return false, response, err
		}
		var status struct {
			Partitions []struct {
				Topic            string   `json:"topic"`
				Partition        int      `json:"partition"`
				LeaderEpoch      int      `json:"leader_epoch"`
				ISR              []string `json:"isr"`
				RecoveryReplicas []string `json:"recovery_replicas"`
				RecoveryPending  bool     `json:"recovery_pending"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
			return false, response, err
		}
		for _, partition := range status.Partitions {
			if partition.Topic != topicName || partition.Partition != partitionID {
				continue
			}
			observedEpoch = partition.LeaderEpoch
			if expectedEpoch != 0 && partition.LeaderEpoch != expectedEpoch {
				return false, fmt.Sprintf("leader_epoch=%d want=%d", partition.LeaderEpoch, expectedEpoch), nil
			}
			if partition.RecoveryPending != pending || len(partition.RecoveryReplicas) != 0 || len(partition.ISR) != expectedISR {
				return false, fmt.Sprintf("leader_epoch=%d recovery_pending=%t recovery_replicas=%v isr=%v", partition.LeaderEpoch, partition.RecoveryPending, partition.RecoveryReplicas, partition.ISR), nil
			}
			return true, fmt.Sprintf("leader_epoch=%d recovery_pending=%t isr=%v", partition.LeaderEpoch, partition.RecoveryPending, partition.ISR), nil
		}
		return false, "partition not found", nil
	}))
	return observedEpoch
}

func truncateWithAmbiguousResponse(t *testing.T, addrs []string, topicName string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	response, requestErr := client.SendCommand("admin", "TRUNCATE topic="+topicName+" expected_revision=1", time.Millisecond)
	client.Close()
	if requestErr == nil {
		requireDefinitionFields(t, response, map[string]string{"truncated": "true", "revision": "2"})
		return
	}

	if err := eventually(t, "ambiguous truncate outcome", clusterReadyTimeout, func() (bool, string, error) {
		probe := e2e.NewBrokerClient(addrs)
		metadata, err := probe.SendCommand("", "METADATA topic="+topicName, 5*time.Second)
		probe.Close()
		if err != nil {
			return false, "metadata unavailable", nil
		}
		revision := topicResponseFields(metadata)["revision"]
		return revision == "2", "revision=" + revision, nil
	}); err == nil {
		return
	}

	response = sendClusterTopicCommand(t, addrs, "TRUNCATE topic="+topicName+" expected_revision=1")
	requireDefinitionFields(t, response, map[string]string{"truncated": "true", "revision": "2"})
}

func requireOffsetResponsesConverge(t *testing.T, addrs []string, topicName string) {
	t.Helper()
	require.NoError(t, eventually(t, "replica offset responses converge for "+topicName, 2*clusterReadyTimeout, func() (bool, string, error) {
		var expected string
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", "LIST_OFFSETS topic="+topicName, 5*time.Second)
			client.Close()
			if err != nil {
				return false, fmt.Sprintf("%s: %v", addr, err), nil
			}
			if expected == "" {
				expected = response
				continue
			}
			if response != expected {
				return false, fmt.Sprintf("%s differs: %s != %s", addr, response, expected), nil
			}
		}
		return true, expected, nil
	}))
}

func requireClusterStatusMaterialized(t *testing.T, addrs []string) {
	t.Helper()
	require.NoError(t, eventually(t, "cluster status materialization health", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
			client.Close()
			if err != nil || !strings.HasPrefix(response, "OK cluster=") {
				return false, fmt.Sprintf("%s response=%q err=%v", addr, response, err), nil
			}
			var status struct {
				Healthy                       bool `json:"healthy"`
				TopicMaterializationPending   int  `json:"topic_materialization_pending"`
				ReplicaMaterializationPending int  `json:"replica_materialization_pending"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
				return false, response, err
			}
			if !status.Healthy || status.TopicMaterializationPending != 0 || status.ReplicaMaterializationPending != 0 {
				return false, fmt.Sprintf("%s healthy=%t topic_pending=%d replica_pending=%d", addr, status.Healthy, status.TopicMaterializationPending, status.ReplicaMaterializationPending), nil
			}
		}
		return true, "all brokers materialized", nil
	}))
}
