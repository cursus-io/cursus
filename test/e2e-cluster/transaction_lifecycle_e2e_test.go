package e2e_cluster

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestChaosProducerReinitializationPreservesCommittedProgress(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run transaction lifecycle validation")
	}
	// Reinitialization must resolve the open transaction itself; the background
	// timeout monitor must not make this regression pass by aborting it first.
	t.Setenv("TRANSACTION_TIMEOUT_MS", "60000")
	ctx := GivenClusterRestart(t).WithClusterSize(3).WithTopic("transaction-lifecycle").WithPartitions(1).WithAcks("all")
	ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()
	check := func(err error) {
		t.Helper()
		var brokerErr *wire.BrokerError
		if errors.As(err, &brokerErr) {
			t.Logf("broker error: code=%s fields=%v", brokerErr.Code, brokerErr.Fields)
		}
		require.NoError(t, err)
	}
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	const txnID = "transaction-lifecycle-producer"
	producer, err := client.InitTransactionProducer(txnID)
	check(err)
	check(client.BeginTransaction(txnID, producer))
	check(client.TransactionalPublish(txnID, ctx.GetTopic(), 0, producer, 1, "abandoned"))
	_, err = client.SendCommand("", "DELETE topic="+ctx.GetTopic(), 3*time.Second)
	require.ErrorContains(t, err, "topic_delete_blocked")
	_, err = client.SendCommand("", "TRUNCATE topic="+ctx.GetTopic()+" expected_revision=1", 3*time.Second)
	require.ErrorContains(t, err, "topic_truncate_blocked")
	status, err := client.GetTransactionStatus(txnID)
	check(err)
	require.Equal(t, "open", status.State)

	next, err := client.InitTransactionProducer(txnID)
	check(err)
	require.Equal(t, producer.ProducerID, next.ProducerID)
	require.Equal(t, producer.Epoch+1, next.Epoch)
	check(client.BeginTransaction(txnID, next))
	check(client.TransactionalPublish(txnID, ctx.GetTopic(), 0, next, 1, "after-restart"))
	check(client.EndTransaction(txnID, next, "commit"))
	reader, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), "transaction-lifecycle-readers")
	defer reader.Close()
	require.Equal(t, []string{"after-restart"}, consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 0, "transaction-lifecycle-readers", member, generation))
}
