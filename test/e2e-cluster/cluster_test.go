package e2e_cluster

import (
	"testing"
	"time"
)

// TestISRWithAllAcks tests ISR behavior with acks=all
func TestISRWithAllAcks(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("isr-test").
		WithPartitions(1).
		WithNumMessages(50).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateFollowerFailure(2).
		Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ISRMaintained())
}

// TestLeaderFailover tests cluster recovery when leader fails
func TestLeaderFailover(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("failover-test").
		WithPartitions(1).
		WithNumMessages(20).
		WithAcks("all")
	defer ctx.Cleanup()

	leaderNode, actions := ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		DescribeTopic().
		PublishMessages().
		SimulateLeaderFailure()

	actions.DescribeTopic().
		RecoverFollower(leaderNode).
		Then().
		Expect(MessagesPublishedWithQuorum())
}

// TestClusterDataConsistency verifies data is replicated correctly after node recovery
func TestClusterDataConsistency(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("consistency-test").
		WithPartitions(1).
		WithNumMessages(10).
		WithAcks("all")
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		SimulateFollowerFailure(3).
		PublishMessages().
		RecoverFollower(3).
		DescribeTopic()

	time.Sleep(5 * time.Second)

	ctx.Then().
		Expect(ExpectDataConsistent())
}

// TestDistributedOffsetResilience verifies committed offsets are preserved after leader failover
func TestDistributedOffsetResilience(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("offset-test").
		WithPartitions(1)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		JoinGroup().
		CommitOffset(0, 50)
	ctx.WhenCluster().SimulateLeaderFailure()

	time.Sleep(5 * time.Second) // wait for election

	ctx.Then().
		Expect(ExpectOffsetMatched(0, 50))
}

// TestRollingRestartNoDowntime simulates rolling restart and verifies zero downtime
func TestRollingRestartNoDowntime(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("rolling-test").
		WithNumMessages(30).
		WithAcks("all")
	defer ctx.Cleanup()
	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages()

	// rolling restart nodes
	for i := 1; i <= 3; i++ {
		ctx.WhenCluster().
			SimulateFollowerFailure(i).
			RecoverFollower(i)
		time.Sleep(5 * time.Second) // allow node to rejoin Raft
	}

	ctx.ResetPublishedCount().
		WithNumMessages(30).
		WhenCluster().
		PublishMessages().
		Then().
		Expect(MessagesPublishedWithQuorum())
}

// TestClusterWideDeduplication verifies exactly-once delivery across cluster failover
func TestClusterWideDeduplication(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("dedup-test").
		WithNumMessages(5).
		WithAcks("all")
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages()
	ctx.WhenCluster().SimulateLeaderFailure()

	time.Sleep(7 * time.Second)

	ctx.WhenCluster().
		RetryPublishMessages().
		Then().
		Expect(ExpectDataConsistent())
}

// TestConsumerGroupRebalanceFailover verifies group stability during node failure
func TestConsumerGroupRebalanceFailover(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("rebalance-test").
		WithPartitions(2)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		JoinGroup().
		SyncGroup()
	ctx.WhenCluster().SimulateLeaderFailure()

	ctx.Then().
		Expect(ISRMaintained())
}
