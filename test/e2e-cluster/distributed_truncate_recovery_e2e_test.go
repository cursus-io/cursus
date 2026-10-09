package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
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
	sagaTopics := []string{"truncate-recovery-state", "truncate-recovery-commands", "truncate-recovery-history"}
	ctx := GivenClusterRestart(t).WithClusterSize(3).WithTopic(businessTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	for _, topicName := range append([]string{businessTopic}, sagaTopics...) {
		sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
			"CREATE topic="+topicName+" partitions=1 replication_factor=3",
		)
		requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
			"revision": "1", "lifecycle_epoch": "1", "partitions": "1", "replication_factor": "3",
		})
	}

	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	for _, topicName := range append([]string{businessTopic}, sagaTopics...) {
		require.NoError(t, client.PublishIdempotentToPartition(topicName, "truncate-recovery-producer", 0, 1, 0, "before-truncate", "all", true))
	}
	client.Close()

	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "TRUNCATE topic="+sagaTopics[0]+" expected_revision=1")
	follower := waitForRaftFollower(t, actions)
	actions.StopBroker(follower)
	available := availableBrokerAddrs(ctx.GetBrokerAddrs(), follower)
	requireFailoverISRReady(t, available, businessTopic, follower, 1)

	truncateWithAmbiguousResponse(t, available, sagaTopics[1])
	sendClusterTopicCommand(t, available, "TRUNCATE topic="+sagaTopics[2]+" expected_revision=1")

	group := "truncate-recovery-during-restart"
	groupClient, generation, member := joinClusterGroup(t, available, businessTopic, group)
	defer groupClient.Close()
	commitResponse, err := groupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=1 generation=%d member=%s",
		businessTopic, group, generation, member,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitResponse, "OK"), commitResponse)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, groupClient, businessTopic, group))

	actions.StartBroker(follower)
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "distributed truncate recovery")
	for _, topicName := range sagaTopics {
		requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
			"revision": "2", "lifecycle_epoch": "2",
		})
		requireClusterPartitionOffsetsEventually(t, ctx.GetBrokerAddrs(), topicName, 0, true)
	}

	require.Equal(t, uint64(1), fetchCommittedOffset(t, groupClient, businessTopic, group))
	commitResponse, err = groupClient.SendCommand("", fmt.Sprintf(
		"COMMIT_OFFSET topic=%s partition=0 group=%s offset=1 generation=%d member=%s",
		businessTopic, group, generation, member,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitResponse, "OK"), commitResponse)
	requireOffsetResponsesConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName)
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	leaveResponse, err := groupClient.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		businessTopic, group, member, generation,
	), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leaveResponse, "OK"), leaveResponse)
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
