package e2e_cluster

import (
	"os"
	"testing"
	"time"
)

// TestChaosLeaderFailoverRetryAndOffsetDurability is intentionally opt-in because
// it restarts Docker cluster members and waits for multiple elections. Run with
// RUN_E2E_CHAOS=1 when validating broker failure semantics before release.
func TestChaosLeaderFailoverRetryAndOffsetDurability(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run long cluster chaos validation")
	}

	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("chaos-eos-test").
		WithPartitions(1).
		WithNumMessages(100).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	leaderNode, actions := ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		CommitOffset(0, 50).
		SimulateLeaderFailure()

	time.Sleep(10 * time.Second)

	actions.RetryPublishMessages().
		RecoverFollower(leaderNode).
		Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ExpectDataConsistent()).
		And(ExpectOffsetMatched(0, 50))
}
