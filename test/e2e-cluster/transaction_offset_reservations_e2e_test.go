package e2e_cluster

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Exercise the reservation RPC over real broker connections: local-only tests
// cannot prove that the group owner sees the replicated transaction decision.
func TestTransactionOffsetsCommitAcrossDifferentCoordinators(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run transaction offset reservation validation")
	}
	t.Setenv("TRANSACTION_TIMEOUT_MS", "60000")
	ctx := GivenClusterRestart(t).WithClusterSize(3).WithTopic("reserved-input").WithPartitions(1).WithAcks("all").WithIdempotent(false)
	ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()
	const group = "reserved-input-workers"
	const output = "reserved-output"
	require.NoError(t, waitForGroupRegistration(ctx.GetTopic(), group))
	reader, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
	defer reader.Close()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	require.NoError(t, client.CreateTopic(output, 1, false))
	require.NoError(t, client.PublishIdempotent(ctx.GetTopic(), "reservation-input-producer", 1, 0, "input", "all", false))
	groupOwner := findGroupCoordinator(t, 1, group)
	var txnID, txnOwner string
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("reservation-cross-owner-%d", i)
		owner, err := client.FindTransactionCoordinator(candidate)
		require.NoError(t, err)
		if owner != groupOwner {
			txnID, txnOwner = candidate, owner
			break
		}
	}
	require.NotEmpty(t, txnID)
	t.Logf("transaction coordinator=%s group coordinator=%s", txnOwner, groupOwner)
	producer, err := client.InitTransactionProducer(txnID)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction(txnID, producer))
	require.NoError(t, client.TransactionalPublish(txnID, output, 0, producer, 1, "processed"))
	require.NoError(t, client.SendOffsetsToTransaction(txnID, ctx.GetTopic(), group, member, generation, producer, map[int]uint64{0: 1}))
	err = client.EndTransaction(txnID, producer, "commit")
	var brokerErr *wire.BrokerError
	if errors.As(err, &brokerErr) {
		t.Logf("END_TXN error code=%s fields=%v", brokerErr.Code, brokerErr.Fields)
	}
	require.NoError(t, err)
	require.NoError(t, client.EndTransaction(txnID, producer, "commit"), "retry must not append duplicate output")
	offset, err := reader.FetchCommittedOffset(ctx.GetTopic(), 0, group)
	require.NoError(t, err)
	require.Equal(t, uint64(1), offset)
	require.NoError(t, waitForGroupRegistration(output, "reserved-output-readers"))
	outputReader, outputGeneration, outputMember := joinClusterGroup(t, ctx.GetBrokerAddrs(), output, "reserved-output-readers")
	defer outputReader.Close()
	require.Equal(t, []string{"processed"}, consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), output, 0, "reserved-output-readers", outputMember, outputGeneration))
	// The consumer offset must survive replacement of the group coordinator,
	// independently of the node that acknowledged the transaction.
	node := coordinatorNode(t, groupOwner)
	stopComposeBroker(t, node)
	survivors := survivorNodes(node)
	waitForSingleRaftLeader(t, survivors, node)
	waitForAllBrokerReadiness(t, survivors)
	recovered := e2e.NewBrokerClient(clusterBrokerAddrsForNodes(survivors))
	defer recovered.Close()
	require.NoError(t, eventually(t, "committed reservation survives coordinator failover", clusterReadyTimeout, func() (bool, string, error) {
		value, fetchErr := recovered.FetchCommittedOffset(ctx.GetTopic(), 0, group)
		return fetchErr == nil && value == 1, fmt.Sprintf("offset=%d err=%v", value, fetchErr), nil
	}))
	startComposeBroker(t, node)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
	require.NoError(t, eventually(t, "completed transaction survives coordinator rejoin", 30*time.Second, func() (bool, string, error) {
		status, statusErr := client.GetTransactionStatus(txnID)
		return statusErr == nil && status.State == "committed" && status.Offsets == 0, fmt.Sprintf("status=%+v err=%v", status, statusErr), nil
	}))
}

func TestPreparedOffsetsRecoverAfterMemberDepartureAndCoordinatorFailure(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run prepared transaction recovery validation")
	}
	t.Setenv("TRANSACTION_TIMEOUT_MS", "60000")
	const input, output, group = "prepared-input", "prepared-output", "prepared-workers"
	ctx := GivenClusterRestart(t).WithClusterSize(3).WithTopic(output).WithPartitions(1).WithAcks("all")
	ctx.WhenCluster().StartCluster()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	require.NoError(t, client.CreateTopic(input, 1, false))
	_, err := client.SendCommand("", "CREATE topic="+output+" partitions=1 min_in_sync_replicas=3", 5*time.Second)
	require.NoError(t, err)
	ctx.WhenCluster().WaitForTopicMetadata()
	waitForFullISRAndZeroUnderReplicated(t, ctx, "prepared transaction setup")
	require.NoError(t, client.PublishIdempotent(input, "prepared-input-producer", 1, 0, "input", "all", false))
	require.NoError(t, waitForGroupRegistration(input, group))
	reader, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), input, group)
	defer reader.Close()
	groupOwner := findGroupCoordinator(t, 1, group)
	var txnID, txnOwner string
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("prepared-recovery-%d", i)
		owner, err := client.FindTransactionCoordinator(candidate)
		require.NoError(t, err)
		if owner != groupOwner {
			txnID, txnOwner = candidate, owner
			break
		}
	}
	require.NotEmpty(t, txnID)
	producer, err := client.InitTransactionProducer(txnID)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction(txnID, producer))
	require.NoError(t, client.TransactionalPublish(txnID, output, 0, producer, 1, "recovered-once"))
	require.NoError(t, client.SendOffsetsToTransaction(txnID, input, group, member, generation, producer, map[int]uint64{0: 1}))
	// Keeping two brokers alive permits Raft and consumer metadata writes,
	// while output minISR=3 prevents completion of the prepared commit.
	offline := 0
	for node := 1; node <= 3; node++ {
		if node != coordinatorNode(t, txnOwner) && node != coordinatorNode(t, groupOwner) {
			offline = node
			break
		}
	}
	require.NotZero(t, offline)
	stopComposeBroker(t, offline)
	survivors := survivorNodes(offline)
	waitForSingleRaftLeader(t, survivors, offline)
	waitForAllBrokerReadiness(t, survivors)
	waitForBrokerEvictedFromISR(t, ctx, offline)
	var brokerErr *wire.BrokerError
	require.NoError(t, eventually(t, "commit prepared while output lacks required replicas", 45*time.Second, func() (bool, string, error) {
		commitErr := client.EndTransaction(txnID, producer, "commit")
		status, statusErr := client.GetTransactionStatus(txnID)
		if commitErr == nil {
			return false, "commit incorrectly succeeded with only two of three required replicas", fmt.Errorf("unexpected commit success")
		}
		if errors.As(commitErr, &brokerErr) {
			t.Logf("pending commit: code=%s fields=%v", brokerErr.Code, brokerErr.Fields)
		}
		return statusErr == nil && status.State == "prepare_commit", fmt.Sprintf("state=%s commit=%v status=%v", status.State, commitErr, statusErr), nil
	}))
	leaveOwner := findGroupCoordinator(t, survivors[0], group)
	leaveClient := e2e.NewBrokerClient([]string{fmt.Sprintf("localhost:%d", brokerPort(coordinatorNode(t, leaveOwner)))})
	defer leaveClient.Close()
	_, err = leaveClient.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", input, group, member, generation), 10*time.Second)
	require.NoError(t, err)
	_, err = leaveClient.FetchCommittedOffset(input, 0, group)
	require.Error(t, err)
	require.True(t, errors.As(err, &brokerErr))
	require.Equal(t, "unstable_offset_commit", brokerErr.Code)
	currentOwner, err := client.FindTransactionCoordinator(txnID)
	require.NoError(t, err)
	failedCoordinator := coordinatorNode(t, currentOwner)
	require.NotEqual(t, offline, failedCoordinator)
	ctx.WhenCluster().KillBroker(failedCoordinator)
	startComposeBroker(t, offline)
	newSurvivors := survivorNodes(failedCoordinator)
	waitForSingleRaftLeader(t, newSurvivors, failedCoordinator)
	waitForAllBrokerReadiness(t, newSurvivors)
	recoveryClient := e2e.NewBrokerClient(clusterBrokerAddrsForNodes(newSurvivors))
	defer recoveryClient.Close()
	require.NoError(t, eventually(t, "prepared transaction reassigned after coordinator failure", clusterReadyTimeout, func() (bool, string, error) {
		owner, findErr := recoveryClient.FindTransactionCoordinator(txnID)
		status, statusErr := recoveryClient.GetTransactionStatus(txnID)
		return findErr == nil && statusErr == nil && owner != currentOwner && status.State == "prepare_commit", fmt.Sprintf("owner=%s state=%s find=%v status=%v", owner, status.State, findErr, statusErr), nil
	}))
	startComposeBroker(t, failedCoordinator)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
	waitForFullISRAndZeroUnderReplicated(t, ctx, "prepared coordinator restart")
	require.NoError(t, eventually(t, "departed member transaction finishes with its durable reservation", clusterReadyTimeout, func() (bool, string, error) {
		status, statusErr := client.GetTransactionStatus(txnID)
		offset, offsetErr := reader.FetchCommittedOffset(input, 0, group)
		return statusErr == nil && offsetErr == nil && status.State == "committed" && status.Offsets == 0 && offset == 1, fmt.Sprintf("state=%s staged=%d offset=%d status=%v fetch=%v", status.State, status.Offsets, offset, statusErr, offsetErr), nil
	}))
	require.NoError(t, client.EndTransaction(txnID, producer, "commit"))
	require.NoError(t, waitForGroupRegistration(output, "prepared-output-readers"))
	outputReader, outputGeneration, outputMember := joinClusterGroup(t, ctx.GetBrokerAddrs(), output, "prepared-output-readers")
	defer outputReader.Close()
	require.Equal(t, []string{"recovered-once"}, consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), output, 0, "prepared-output-readers", outputMember, outputGeneration))
}
