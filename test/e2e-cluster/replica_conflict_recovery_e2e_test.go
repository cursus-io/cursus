package e2e_cluster

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Reproduce the durable follower tail left when a replication attempt fails
// permanently and the leader releases its uncommitted offset reservation.
// The next append uses the same epoch and offset but a different record.
func TestReplicaConflictRecoveryAfterPartialAppend(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run replica conflict recovery validation")
	}
	for _, mode := range []struct{ name, acks string }{{"all", "all"}, {"leader", "1"}} {
		t.Run(mode.name, func(t *testing.T) { verifyReplicaConflictRecovery(t, mode.acks) })
	}
}

func verifyReplicaConflictRecovery(t *testing.T, acks string) {
	const history = "conflict-recovery-history"
	const inbox = "conflict-recovery-inbox"
	ctx := GivenFaultClusterRestart(t).WithClusterSize(3).WithTopic(history).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	for _, topicName := range []string{history, inbox} {
		sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+topicName+" partitions=1 replication_factor=3")
		requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{"revision": "1", "replication_factor": "3"})
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		require.NoError(t, client.PublishIdempotentToPartition(topicName, "seed", 0, 1, 0, "committed-prefix", "all", true))
		client.Close()
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), topicName, 1)
	}
	epoch := requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, 0)
	for node := 1; node <= 3; node++ {
		setReplicaFaultSentinel(t, node, "replica-append-failure", history+"-0-1", true)
		setReplicaCatchupPauseFault(t, node, history, true)
		defer setReplicaCatchupPauseFaultBestEffort(node, history, false)
	}
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	response, err := client.SendCommand("", "PUBLISH topic="+history+" partition=0 acks=all producerId=failed message=uncommitted-tail", 15*time.Second)
	require.ErrorContains(t, err, "replica_index_failed")
	divergent := 0
	for node, addr := range ctx.GetBrokerAddrs() {
		local := e2e.NewBrokerClient([]string{addr})
		offsets, err := local.SendCommand("", "LIST_OFFSETS topic="+history+" partition=0", 5*time.Second)
		local.Close()
		require.NoError(t, err)
		leo, hwm, err := parsePartitionOffsets(offsets, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), hwm, "failed append must not commit")
		if leo == 2 {
			divergent++
		} else {
			require.Equal(t, uint64(1), leo)
		}
		setReplicaFaultSentinel(t, node+1, "replica-append-failure", history+"-0-1", false)
	}
	require.Equal(t, 1, divergent, "the first follower retains the failed append; replication stops before the second")
	response, err = client.SendCommand("", "PUBLISH topic="+history+" partition=0 acks="+acks+" producerId=replacement message=replacement", 15*time.Second)
	if acks == "all" {
		require.True(t, err != nil || strings.HasPrefix(response, "ERROR"), "must not commit over the divergent tail even with sufficient minISR: %s", response)
	} else {
		require.NoError(t, err, "leader acknowledgement must retain its accepted record during recovery")
	}
	recoveryNode := requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), history, 0, epoch)
	t.Log("same-epoch conflict quarantined; readiness closed while catch-up is paused")
	actions.StopBroker(recoveryNode)
	restartBrokerDuringRecovery(t, recoveryNode)
	requireRecoveryPendingAndNotReady(t, ctx.GetBrokerAddrs(), history, 0, epoch)

	// Consumer metadata remains usable on unrelated partitions during recovery.
	const group = "conflict-recovery-consumer"
	groupClient, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), inbox, group)
	defer groupClient.Close()
	commitGroupOffset(t, groupClient, inbox, group, 1, generation, member)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, groupClient, inbox, group))
	for node := 1; node <= 3; node++ {
		setReplicaCatchupPauseFault(t, node, history, false)
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "same-epoch replica conflict recovery")
	requirePartitionRecoveryState(t, ctx.GetBrokerAddrs(), history, 0, false, 3, epoch)
	if acks == "all" {
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 1)
		require.NoError(t, client.PublishIdempotentToPartition(history, "replacement", 0, 1, 0, "replacement", "all", true))
	}
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), history, 2)

	historyClient, historyGeneration, historyMember := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), history, "conflict-history-reader")
	defer historyClient.Close()
	records := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), history, 0, "conflict-history-reader", historyMember, historyGeneration)
	require.Len(t, records, 2)
	require.Contains(t, strings.Join(records, " "), "committed-prefix")
	require.Contains(t, strings.Join(records, " "), "replacement")
	require.NotContains(t, strings.Join(records, " "), "uncommitted-tail")
	// Observe more than twice the fixture's 30-second session timeout.
	deadline := time.Now().Add(65 * time.Second)
	for time.Now().Before(deadline) {
		heartbeatGroup(t, groupClient, inbox, group, generation, member)
		heartbeatGroup(t, historyClient, history, "conflict-history-reader", historyGeneration, historyMember)
		status, err := groupClient.GetConsumerGroupStatus(group)
		require.NoError(t, err)
		require.Equal(t, generation, status.Generation)
		require.Len(t, status.Members, 1)
		require.Equal(t, uint64(1), fetchCommittedOffset(t, groupClient, inbox, group))
		time.Sleep(5 * time.Second)
	}
	leave, err := groupClient.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", inbox, group, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leave, "OK"), leave)
	rejoined, _, _ := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), inbox, group)
	defer rejoined.Close()
	require.Equal(t, uint64(1), fetchCommittedOffset(t, rejoined, inbox, group))
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
	t.Log("prefix preserved, full ISR restored, writes resumed, and consumer generation/offset stable for 65 seconds")
}
