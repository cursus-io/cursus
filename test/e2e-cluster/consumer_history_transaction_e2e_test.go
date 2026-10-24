package e2e_cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryTransactionsAcrossCoordinatorMove(t *testing.T) {
	if os.Getenv("RUN_E2E_CONSUMER_HISTORY") != "1" {
		t.Skip("opt-in isolated history cluster")
	}
	require.NotZero(t, cleanupPortOffset())
	t.Setenv("CURSUS_HISTORY_MEASURE_COMPACTION", "true")
	t.Setenv("CURSUS_HISTORY_TRACE_SYNC", "false")
	t.Setenv("TRANSACTION_TIMEOUT_MS", "60000")
	ctx := givenCleanupCluster(t, composeFile, "docker-compose.consumer-history.yml", "docker-compose.history-measure.yml").WithClusterSize(3).WithTopic("history-transactions").WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-transactions partitions=1 replication_factor=3")
	const group, id = "history-transaction-group", "history-transaction-id"
	reader, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-transactions", group)
	defer reader.Close()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	start := time.Now()
	for epoch := 1; epoch <= 100; epoch++ {
		if epoch == 1 || epoch%10 == 0 {
			t.Logf("processing transaction epoch %d", epoch)
		}
		producer, err := client.InitTransactionProducer(id)
		require.NoError(t, err)
		require.NoError(t, client.BeginTransaction(id, producer))
		require.NoError(t, client.SendOffsetsToTransaction(id, "history-transactions", group, member, generation, producer, map[int]uint64{0: uint64(epoch)}))
		require.NoError(t, eventually(t, "transaction commit during history maintenance", 30*time.Second, func() (bool, string, error) {
			err := client.EndTransaction(id, producer, "commit")
			return err == nil, fmt.Sprint(err), nil
		}))
		require.NoError(t, client.EndTransaction(id, producer, "commit"), "terminal retry must remain idempotent")
		consumerHistoryCommand(t, reader, fmt.Sprintf("HEARTBEAT topic=history-transactions group=%s member=%s generation=%d", group, member, generation))
		require.Equal(t, uint64(epoch), fetchCommittedOffset(t, reader, "history-transactions", group))
		if epoch == 50 {
			owner, err := client.FindTransactionCoordinator(id)
			require.NoError(t, err)
			node := coordinatorNode(t, owner)
			historyMeasureDocker(t, "stop", cleanupContainerName(node))
			survivors := survivorNodes(node)
			survivor := e2e.NewBrokerClient(clusterBrokerAddrsForNodes(survivors))
			require.NoError(t, eventually(t, "transaction history coordinator replacement", clusterReadyTimeout, func() (bool, string, error) {
				next, err := survivor.FindTransactionCoordinator(id)
				return err == nil && next != owner, fmt.Sprintf("owner=%s err=%v", next, err), nil
			}))
			require.Equal(t, uint64(epoch), fetchCommittedOffset(t, survivor, "history-transactions", group))
			survivor.Close()
			historyMeasureDocker(t, "start", cleanupContainerName(node))
			require.NoError(t, actions.checkAllNodesHealth())
			waitForStableFullISRAndZeroUnderReplicated(t, ctx, "transaction history owner return")
		}
	}
	t.Logf("100 committed transaction epochs including ownership move wall=%s", time.Since(start))
	require.NoError(t, eventually(t, "retired transaction intents actually leave every replica", 2*time.Minute, func() (bool, string, error) {
		consumerHistoryCommand(t, reader, fmt.Sprintf("HEARTBEAT topic=history-transactions group=%s member=%s generation=%d", group, member, generation))
		for node := 1; node <= 3; node++ {
			data := historyMeasureDocker(t, "exec", cleanupContainerName(node), "python3", "/app/history_measure.py", "transaction-count")
			count, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				return false, "count", err
			}
			if count == 0 || count > 8 {
				return false, fmt.Sprintf("node=%d intents=%d", node, count), nil
			}
		}
		return true, "bounded retained transaction evidence", nil
	}))
	consumerHistoryCommand(t, reader, fmt.Sprintf("LEAVE_GROUP topic=history-transactions group=%s member=%s generation=%d", group, member, generation))
	for node := 1; node <= 3; node++ {
		historyMeasureDocker(t, "stop", cleanupContainerName(node))
		historyMeasureOffline(t, node, "absent", "cold")
		historyMeasureDocker(t, "start", cleanupContainerName(node))
		require.NoError(t, actions.waitForNodeHealth(node, fmt.Sprintf("http://localhost:%d/ready", healthPort(node))))
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, "transaction cache miss")
		require.Equal(t, uint64(100), fetchCommittedOffset(t, client, "history-transactions", group))
	}
}
