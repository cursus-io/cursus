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
	"github.com/cursus-io/cursus/test/e2e"
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
	seedGroupClient, seedGeneration, seedMember := joinClusterGroup(t, ctx.GetBrokerAddrs(), businessTopic, group)
	commitResponse, err := seedGroupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=1 generation=%d member=%s",
		businessTopic, group, seedGeneration, seedMember,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitResponse, "OK"), commitResponse)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, seedGroupClient, businessTopic, group))
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

	leaderEpoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), businessTopic, false, 3, 0)
	for node := 1; node <= 3; node++ {
		setReplicaAppendSkipFault(t, node, businessTopic, 0, 1)
		setReplicaCatchupPauseFault(t, node, businessTopic, true)
		defer setReplicaCatchupPauseFaultBestEffort(node, businessTopic, false)
	}
	gapClient := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	require.NoError(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 2, 0, "before-gap", "all", true))
	require.Error(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 3, 0, "gap-trigger", "all", true))
	recoveryNode := requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), businessTopic, leaderEpoch)
	actions.StopBroker(recoveryNode)
	restartBrokerDuringRecovery(t, recoveryNode)
	requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), businessTopic, leaderEpoch)
	require.Error(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 3, 0, "blocked-during-recovery", "all", true))
	for node := 1; node <= 3; node++ {
		setReplicaCatchupPauseFault(t, node, businessTopic, false)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "replica gap recovery")
	requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), businessTopic, false, 3, leaderEpoch)
	require.NoError(t, gapClient.PublishIdempotentToPartition(businessTopic, "truncate-recovery-producer", 0, 3, 0, "after-recovery", "all", true))
	gapClient.Close()
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), businessTopic, 3)

	groupClient, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), businessTopic, group)
	defer groupClient.Close()
	require.Equal(t, uint64(1), fetchCommittedOffset(t, groupClient, businessTopic, group))
	commitResponse, err = groupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=3 generation=%d member=%s",
		businessTopic, group, generation, member,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitResponse, "OK"), commitResponse)
	require.Equal(t, uint64(3), fetchCommittedOffset(t, groupClient, businessTopic, group))
	requireOffsetResponsesConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName)
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	leaveResponse, err := groupClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, group, member, generation,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
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

func requireRecoveryPendingAndNotReady(t *testing.T, addrs []string, topicName string, leaderEpoch int) int {
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
			if partition.Topic != topicName {
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

func requirePartitionRecoveryState(t *testing.T, addrs []string, topicName string, pending bool, expectedISR int, expectedEpoch int) int {
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
			if partition.Topic != topicName {
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
