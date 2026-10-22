package e2e_cluster

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Restart followers while a member is live, then leave through the owner.
// Followers must use the owner's committed deletion, not their stale members,
// to finish the later topic intent.
func TestCommittedConsumerCleanupConvergesOnFollowers(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run consumer cleanup convergence")
	}
	ctx := givenCleanupCluster(t, composeFile, "docker-compose.consumer-cleanup.yml").
		WithClusterSize(3).WithTopic("cleanup-orders").WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	const topicName, group = "cleanup-orders", "cleanup-workers"
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+topicName+" partitions=1 replication_factor=3")
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{"revision": "1"})
	client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), topicName, group)
	defer client.Close()
	owner := findGroupCoordinator(t, 1, group)
	restarted := 0
	for node := 1; node <= 3; node++ {
		if owner == fmt.Sprintf("broker-%d-9000", node) {
			continue
		}
		heartbeatGroup(t, client, topicName, group, generation, member)
		name := cleanupContainerName(node)
		output, err := exec.Command("docker", "restart", name).CombinedOutput()
		require.NoError(t, err, string(output))
		require.NoError(t, actions.waitForNodeHealth(node, fmt.Sprintf("http://localhost:%d/ready", healthPort(node))))
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, "follower recovered live consumer membership")
		restarted++
	}
	require.Equal(t, 2, restarted, "the two non-owning brokers must recover the live group")
	const unrelatedTopic, unrelatedGroup = "cleanup-payments", "cleanup-live"
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+unrelatedTopic+" partitions=1 replication_factor=3")
	live, liveGeneration, liveMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), unrelatedTopic, unrelatedGroup)
	defer live.Close()
	commitGroupOffset(t, live, unrelatedTopic, unrelatedGroup, 17, liveGeneration, liveMember)
	heartbeatGroup(t, client, topicName, group, generation, member)
	response, err := client.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		topicName, group, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)

	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "TRUNCATE topic="+topicName+" expected_revision=1")
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{"revision": "2", "lifecycle_epoch": "2"})
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	heartbeatGroup(t, live, unrelatedTopic, unrelatedGroup, liveGeneration, liveMember)
	require.Equal(t, uint64(17), fetchCommittedOffset(t, live, unrelatedTopic, unrelatedGroup))

	// A recreated group must have a new durable lifecycle, and normal produce,
	// commit and heartbeat must work after all replicas materialize.
	producer := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer producer.Close()
	require.NoError(t, producer.PublishIdempotentToPartition(topicName, "cleanup-after", 0, 1, 0, "after", "all", true))
	recreated, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), topicName, group)
	defer recreated.Close()
	heartbeatGroup(t, recreated, topicName, group, generation, member)
	commitGroupOffset(t, recreated, topicName, group, 1, generation, member)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, recreated, topicName, group))
	response, err = recreated.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		topicName, group, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic="+topicName)
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	heartbeatGroup(t, live, unrelatedTopic, unrelatedGroup, liveGeneration, liveMember)
	require.Equal(t, uint64(17), fetchCommittedOffset(t, live, unrelatedTopic, unrelatedGroup))
	response, err = live.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		unrelatedTopic, unrelatedGroup, liveMember, liveGeneration), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic="+unrelatedTopic)
}
