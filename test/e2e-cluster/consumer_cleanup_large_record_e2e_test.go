package e2e_cluster

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Setup restarts followers and waits for ISR convergence, which can exceed a
// session timeout on a busy host. Keep the real member alive without changing
// broker timeouts or the unrelated session whose heartbeat replay is asserted.
func keepCleanupSetupMemberAlive(t *testing.T, addrs []string, generation int, member string) func() {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		defer close(done)
		client := e2e.NewBrokerClient(addrs)
		defer client.Close()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				response, err := client.SendCommand("", fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", cleanupRecoveryTopic, cleanupRecoveryGroup, member, generation), 15*time.Second)
				if err == nil && !strings.HasPrefix(response, "OK") {
					err = fmt.Errorf("setup heartbeat: %.200s", response)
				}
				if err != nil {
					select {
					case failures <- err:
					default:
					}
				}
			}
		}
	}()
	var once sync.Once
	stopHeartbeat := func() { once.Do(func() { close(stop); <-done }) }
	t.Cleanup(stopHeartbeat)
	return func() {
		stopHeartbeat()
		select {
		case err := <-failures:
			require.NoError(t, err, "setup heartbeat must keep the target member alive")
		default:
		}
	}
}

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
