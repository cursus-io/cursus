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
		WaitForTopicReplication(3, 3).
		PublishMessages().
		Then().
		Expect(ExpectReplicatedMessages(1, 3, 2, 50))
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
		WaitForTopicReplication(3, 3).
		DescribeTopic().
		PublishMessages().
		SimulateLeaderFailure()

	actions.RecoverFollower(leaderNode).
		WaitForTopicMetadata().
		Then().
		Expect(ExpectReplicatedMessages(1, 3, 1, 20))
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
		WaitForTopicReplication(3, 3).
		PublishMessages()
	ctx.WhenCluster().SimulateLeaderFailure()

	ctx.WhenCluster().
		RetryPublishMessages().
		Then().
		Expect(ExpectReplicatedMessages(1, 3, 1, 5))
}
