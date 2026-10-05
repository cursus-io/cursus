package e2e_cluster

import (
	"testing"
)

// TestISRWithAllAcks tests ISR behavior with acks=all
func TestISRWithAllAcks(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("isr-test").
		WithPartitions(1).
		WithNumMessages(50).
		WithIdempotent(true).
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
		WithIdempotent(true).
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
		WithIdempotent(true).
		WithAcks("all")
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		SimulateFollowerFailure(3).
		PublishMessages().
		RecoverFollower(3).
		DescribeTopic()

	ctx.Then().
		Expect(ExpectDataConsistent())
}

// TestClusterWideDeduplication verifies exactly-once delivery across cluster failover
func TestClusterWideDeduplication(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("dedup-test").
		WithNumMessages(5).
		WithIdempotent(true).
		WithAcks("all")
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages()
	ctx.WhenCluster().SimulateLeaderFailure()

	ctx.WhenCluster().
		RetryPublishMessages().
		Then().
		Expect(ExpectDataConsistent())
}
