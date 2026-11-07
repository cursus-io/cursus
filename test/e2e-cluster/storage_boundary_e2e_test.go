package e2e_cluster

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAcknowledgedWritesSurviveStorageErrors verifies acknowledged history survives injected append or sync failure and broker restart.
func TestAcknowledgedWritesSurviveStorageErrors(t *testing.T) {
	if os.Getenv("RUN_E2E_RECOVERY_BOUNDARIES") != "1" {
		t.Skip("set RUN_E2E_RECOVERY_BOUNDARIES=1 for deterministic recovery faults")
	}
	for _, point := range []string{"disk-append", "disk-sync"} {
		t.Run(point, func(t *testing.T) {
			journal := newAckFaultJournal(t)
			ctx := newAckFaultClusterMode(t, "ack-"+point, true)
			state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
			node := coordinatorNode(t, state.Leader)
			for seq := uint64(1); seq <= 8; seq++ {
				publishAckFaultEventually(t, journal, ctx.GetTopic(), node, ackFaultMessage("producer", seq))
			}
			path := armRecoveryBoundary(t, node, point, ctx.GetTopic()+"-0-8")
			failed, sent := journal.publish(ctx.GetBrokerAddrs()[node-1], ctx.GetTopic(), ackFaultMessage("producer", 9))
			require.True(t, sent)
			require.NotEqual(t, "acknowledged", failed.Outcome)
			waitRecoveryBoundary(t, node, path, journal)
			retry, _ := journal.publish(ctx.GetBrokerAddrs()[node-1], ctx.GetTopic(), ackFaultMessage("producer", 9))
			require.NotEqual(t, "acknowledged", retry.Outcome, "failed handler must not bypass terminal error")
			captureAckFaultMetrics(t, journal, "before-storage-restart")
			ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(node))
			next := waitAckFaultState(t, ctx.GetTopic(), ackFaultOtherNodes(node), state.Leader, 2)
			publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, next.Leader), ackFaultMessage("producer", 9))
			ackFaultDocker(t, "start", ackFaultContainer(node))
			verifyAckFaultRecovery(t, ctx, journal)
		})
	}
}
