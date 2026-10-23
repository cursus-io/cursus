package e2e_cluster

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Opt in to a large historical record in the existing destructive fault
// scenarios. No fixture endpoint or disk mutation creates this metadata: the
// ordinary REGISTER/JOIN/LEAVE path writes and replicates its lifecycle state.
func seedLargeCleanupHistory(t *testing.T, ctx *ClusterTestContext) {
	t.Helper()
	if os.Getenv("RUN_E2E_CLEANUP_LARGE") != "1" {
		return
	}
	addrs := ctx.GetBrokerAddrs()
	const group = "cleanup-large-history"
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("", "REGISTER_GROUP topic="+cleanupRetainedMessages+" group="+group, 30*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
	// The API currently accepts a long token. This deliberately synthetic
	// state avoids creating thousands of local partitions just to cross 1 MiB.
	member := "large-" + strings.Repeat("m", (1<<20)+4096)
	response, err = client.SendCommand("", fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", cleanupRetainedMessages, group, member), 30*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), "join response prefix: %.200s", response)
	status, err := client.GetConsumerGroupStatus(group)
	require.NoError(t, err)
	require.Len(t, status.Members, 1)
	require.Greater(t, len(status.Members[0].MemberID), 1<<20)
	partition, _ := consumerOffsetPartition(t, addrs, group)
	requireReplicaPartitionOffsetsConverge(t, addrs, config.ConsumerOffsetsTopicName, partition)
	response, err = client.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", cleanupRetainedMessages, group, status.Members[0].MemberID, status.Generation), 30*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), "leave response prefix: %.200s", response)
	requireReplicaPartitionOffsetsConverge(t, addrs, config.ConsumerOffsetsTopicName, partition)
	t.Logf("normal admission replicated large historical member: id_bytes=%d partition=%d", len(status.Members[0].MemberID), partition)
}
