package e2e_cluster

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
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
	defer func() {
		if t.Failed() && os.Getenv("CURSUS_E2E_DUMP_STACKS") == "1" {
			for node := 1; node <= 3; node++ {
				out, err := exec.Command("docker", "exec", cleanupContainerName(node), "python3", "-c", "import history_measure as h, os, signal; p=h.broker_pid(); os.kill(int(p.name), signal.SIGQUIT) if p else None").CombinedOutput()
				t.Logf("failed-fixture stack dump node=%d: %s err=%v", node, out, err)
				logs, logErr := exec.Command("docker", "logs", cleanupContainerName(node)).CombinedOutput()
				t.Logf("complete failed-fixture logs node=%d err=%v:\n%s", node, logErr, logs)
			}
		}
	}()
	actions := ctx.WhenCluster().StartCluster()
	sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic=history-transactions partitions=1 replication_factor=3")
	const group, id = "history-transaction-group", "history-transaction-id"
	reader, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), "history-transactions", group)
	defer func() { reader.Close() }()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer func() { client.Close() }()
	start := time.Now()
	availabilityRetries := 0
	for epoch := 1; epoch <= 100; epoch++ {
		if epoch == 1 || epoch%10 == 0 {
			t.Logf("processing transaction epoch %d", epoch)
		}
		var producer e2e.TransactionProducer
		availabilityRetries += retryHistoryAdmission(t, func() error {
			var err error
			producer, err = client.InitTransactionProducer(id)
			return err
		})
		require.NoError(t, client.BeginTransaction(id, producer))
		availabilityRetries += retryHistoryAdmission(t, func() error {
			return client.SendOffsetsToTransaction(id, "history-transactions", group, member, generation, producer, map[int]uint64{0: uint64(epoch)})
		})
		require.NoError(t, eventually(t, "transaction commit during history maintenance", 30*time.Second, func() (bool, string, error) {
			err := client.EndTransaction(id, producer, "commit")
			return err == nil, fmt.Sprint(err), nil
		}))
		availabilityRetries += retryHistoryAdmission(t, func() error {
			return client.EndTransaction(id, producer, "commit")
		})
		consumerHistoryCommand(t, reader, fmt.Sprintf("HEARTBEAT topic=history-transactions group=%s member=%s generation=%d", group, member, generation))
		require.Equal(t, uint64(epoch), fetchHistoryOffset(t, reader, "history-transactions", group))
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
			requireHistoryCommittedOffset(t, survivor, "history-transactions", group, uint64(epoch))
			survivor.Close()
			historyMeasureDocker(t, "start", cleanupContainerName(node))
			require.NoError(t, actions.checkAllNodesHealth())
			waitForStableFullISRAndZeroUnderReplicated(t, ctx, "transaction history owner return")
			// INIT is not safe to retry after an ambiguous socket read. Retire
			// connections to the deliberately stopped process before starting
			// the next epoch; keep the same transactional ID and group member.
			client.Close()
			client = e2e.NewBrokerClient(ctx.GetBrokerAddrs())
			reader.Close()
			reader = e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		}
	}
	t.Logf("100 committed transaction epochs including ownership move wall=%s admission_retries=%d", time.Since(start), availabilityRetries)
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
		requireHistoryCommittedOffset(t, client, "history-transactions", group, 100)
	}
}

// Retry only the pre-admission maintenance rejection. Epoch/member failures and
// transport ambiguity must fail the test instead of silently reinitializing.
func retryHistoryAdmission(t *testing.T, operation func() error) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for retries := 0; ; retries++ {
		err := operation()
		if err == nil {
			return retries
		}
		var rejected *wire.BrokerError
		if !errors.As(err, &rejected) || rejected.Code != "consumer_metadata_recovering" || time.Now().After(deadline) {
			if rejected != nil {
				t.Logf("history command rejected code=%s fields=%v", rejected.Code, rejected.Fields)
			}
			require.NoError(t, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Transaction-shard election can precede group-owner recovery. Count that wait
// as failover time, and reject a successful response with any changed offset.
func requireHistoryCommittedOffset(t *testing.T, client *e2e.BrokerClient, topic, group string, expected uint64) {
	t.Helper()
	require.NoError(t, eventually(t, "exact history offset after ownership recovery", clusterReadyTimeout, func() (bool, string, error) {
		offset, err := client.FetchCommittedOffset(topic, 0, group)
		if err != nil {
			return false, fmt.Sprint(err), nil
		}
		if offset != expected {
			return false, "committed offset changed", fmt.Errorf("got=%d want=%d", offset, expected)
		}
		return true, "exact committed offset", nil
	}))
}
