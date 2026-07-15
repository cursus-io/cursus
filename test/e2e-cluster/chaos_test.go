package e2e_cluster

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/test/e2e"
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
	ctx.WithIdempotent(true)
	defer ctx.Cleanup()

	leaderNode, actions := ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		CommitOffset(0, 50).
		SimulateLeaderFailure()

	actions.RetryPublishMessages().
		RecoverFollower(leaderNode)
	actions.Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ExpectDataConsistent()).
		And(ExpectOffsetMatched(0, 50)).
		And(ExpectPartitionWatermarks(0, 100, 100)).
		And(e2e.PublisherRetriedSuccessfully())

	ctx.WithConsumerGroup("chaos-readback-group")
	ctx.WhenCluster().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(e2e.MessagesConsumed(100)).
		And(e2e.NoDuplicateMessages())
}
