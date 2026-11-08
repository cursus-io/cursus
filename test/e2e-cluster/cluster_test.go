package e2e_cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
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
		WithMinInSyncReplicas(3)
	defer ctx.Cleanup()

	actions := ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		WaitForTopicReplication(3, 3).
		PublishMessages()
	actions.Then().
		Expect(ExpectReplicatedMessages(1, 3, 3, 3, 50))

	leaderNode := currentTopicLeaderNode(t, ctx)
	stoppedFollowers := make([]int, 0, 1)
	for node := 1; node <= 3; node++ {
		if node == leaderNode {
			continue
		}
		actions.StopBroker(node)
		stoppedFollowers = append(stoppedFollowers, node)
		break
	}
	actions.Then().
		Expect(ExpectISRBelowMinISR(1, 3, 2, 3, 50))

	ctx.ResetPublishedCount().WithNumMessages(1)
	actions.PublishMessages().
		Then().
		Expect(PublishRejectedBelowMinISR(3))

	for _, node := range stoppedFollowers {
		actions.RecoverFollower(node)
	}
	actions.Then().
		Expect(ExpectTopicReplication(1, 3, 3, 3, 50))
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
		Expect(ExpectReplicatedMessages(1, 3, 3, 2, 20))

	readbackClient, generation, memberID := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), ctx.GetConsumerGroup())
	defer readbackClient.Close()
	messages := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 0, ctx.GetConsumerGroup(), memberID, generation)
	requireTestMessagesExactlyOnce(t, messages, 20)
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
	leaderNode, actions := ctx.WhenCluster().SimulateLeaderFailure()

	actions.RetryPublishMessages().
		RecoverFollower(leaderNode).
		Then().
		Expect(e2e.PublisherRetriedSuccessfully()).
		And(ExpectReplicatedMessages(1, 3, 3, 2, 5))

	readbackClient, generation, memberID := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), ctx.GetConsumerGroup())
	defer readbackClient.Close()
	messages := consumeFromPartitionLeader(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), 0, ctx.GetConsumerGroup(), memberID, generation)
	requireTestMessagesExactlyOnce(t, messages, 5)
}

func currentTopicLeaderNode(t *testing.T, ctx *ClusterTestContext) int {
	t.Helper()
	resp, err := ctx.GetClient().SendCommand("", "DESCRIBE topic="+ctx.GetTopic(), 5*time.Second)
	if err != nil {
		t.Fatalf("describe topic %s: %v", ctx.GetTopic(), err)
	}
	leaderNode, err := leaderNodeFromDescribe(resp, ctx.clusterSize)
	if err != nil {
		t.Fatalf("identify leader for topic %s: %v", ctx.GetTopic(), err)
	}
	return leaderNode
}

func requireTestMessagesExactlyOnce(t *testing.T, messages []string, expected int) {
	t.Helper()
	counts := make(map[string]int, len(messages))
	for _, message := range messages {
		counts[message]++
	}
	if len(messages) != expected {
		t.Fatalf("consumed %d messages, want %d: %v", len(messages), expected, messages)
	}
	for i := 0; i < expected; i++ {
		payload := fmt.Sprintf("test-message-%d", i)
		if counts[payload] != 1 {
			t.Fatalf("payload %q occurred %d times, want exactly once; messages=%v", payload, counts[payload], messages)
		}
		delete(counts, payload)
	}
	if len(counts) != 0 {
		t.Fatalf("consumed unexpected payloads: %v", counts)
	}
}
