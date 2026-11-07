package e2e

import (
	"testing"
)

// TestBasicPubSub verifies basic publish-subscribe functionality
func TestBasicPubSub(t *testing.T) {

	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("test-topic").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(BrokerIsHealthy(StandAloneHealthCheckAddr)).
		And(MessagesPublished(10)).
		And(MessagesConsumed(10))
}

// TestMultiPartition verifies multi-partition behavior
func TestMultiPartition(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("multi-partition-test").
		WithPartitions(4).
		WithNumMessages(20).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesPublished(20)).
		And(MessagesConsumed(20))
}
