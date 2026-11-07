package e2e_cluster

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func armRecoveryBoundary(t *testing.T, node int, point, key string) string {
	t.Helper()
	path := "/run/cursus-e2e-faults/" + point + "/" + key
	ackFaultDocker(t, "exec", ackFaultContainer(node), "mkdir", "-p", "/run/cursus-e2e-faults/"+point)
	ackFaultDocker(t, "exec", ackFaultContainer(node), "touch", path)
	return path
}

func waitRecoveryBoundary(t *testing.T, node int, path string, journal *ackFaultJournal) {
	t.Helper()
	require.NoError(t, eventually(t, "fault boundary entered", 30*time.Second, func() (bool, string, error) {
		state := ackFaultDocker(t, "exec", ackFaultContainer(node), "sh", "-c", "if test -f "+path+".entered; then echo entered; else echo waiting; fi")
		return state == "entered", state, nil
	}))
	journal.record("fault_boundary_entered", map[string]any{"node": node, "path": path})
}

func TestAcknowledgedWritesSurviveCommitApplicationCrash(t *testing.T) {
	if os.Getenv("RUN_E2E_RECOVERY_BOUNDARIES") != "1" {
		t.Skip("set RUN_E2E_RECOVERY_BOUNDARIES=1 for deterministic recovery faults")
	}
	journal := newAckFaultJournal(t)
	ctx := newAckFaultClusterMode(t, "ack-commit-boundary", true)
	state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	leader := coordinatorNode(t, state.Leader)
	raftLeader := waitForSingleRaftLeader(t, []int{1, 2, 3}, 0)
	follower := 0
	for node := 1; node <= 3; node++ {
		if node != leader && node != raftLeader {
			follower = node
			break
		}
	}
	require.NotZero(t, follower)
	for seq := uint64(1); seq <= 8; seq++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), leader, ackFaultMessage("producer", seq))
	}
	waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	path := armRecoveryBoundary(t, follower, "partition-commit", ctx.GetTopic()+"-0-9")
	publishAckFaultEventually(t, journal, ctx.GetTopic(), leader, ackFaultMessage("producer", 9))
	waitRecoveryBoundary(t, follower, path, journal)
	response, _, err := ackFaultRoundTrip(ctx.GetBrokerAddrs()[follower-1], "LIST_OFFSETS topic="+ctx.GetTopic()+" partition=0")
	require.NoError(t, err)
	leo, hwm, err := parsePartitionOffsets(string(response), 0)
	require.NoError(t, err)
	require.Equal(t, uint64(9), leo)
	require.Equal(t, uint64(8), hwm)
	journal.record("durable_data_before_commit_application", map[string]any{"node": follower, "leo": leo, "hwm": hwm})
	captureAckFaultMetrics(t, journal, "before-commit-crash")
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(leader), ackFaultContainer(follower))
	for _, node := range []int{leader, follower} {
		require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(node)))
	}
	ackFaultDocker(t, "start", ackFaultContainer(follower))
	next := waitAckFaultState(t, ctx.GetTopic(), ackFaultOtherNodes(leader), state.Leader, 2)
	require.GreaterOrEqual(t, next.CommittedHWM, uint64(9))
	journal.record("commit_replayed_after_restart", next)
	publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, next.Leader), ackFaultMessage("producer", 10))
	ackFaultDocker(t, "start", ackFaultContainer(leader))
	verifyAckFaultRecovery(t, ctx, journal)
	t.Logf("replayed follower %d with original leader %d unavailable", follower, leader)
}
