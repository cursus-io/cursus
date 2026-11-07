package e2e

import (
	"testing"
)

func TestIdempotentPublishSurvivesBrokerRestart(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("retry-test").
		WithPartitions(1).
		WithNumMessages(5).
		WithAcks("all").
		WithIdempotent(true).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		StopBroker().
		StartBroker().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(PublisherRetriedSuccessfully()).
		And(MessagesConsumed(5)).
		And(NoDuplicateMessages())
}

func TestIdempotentRetryAfterClientReconnect(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("exactly-once-test").
		WithPartitions(1).
		WithNumMessages(10).
		WithAcks("all").
		WithIdempotent(true).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		DisconnectClient().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10)).
		And(NoDuplicateMessages())
}

// TestPublisherAcks verifies different ACK modes
func TestPublisherAcks(t *testing.T) {
	testCases := []struct {
		name string
		acks string
	}{
		{"acks=0", "0"},
		{"acks=1", "1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := GivenStandalone(t)
			defer ctx.Cleanup()

			ctx.WithTopic("acks-test-" + tc.acks).
				WithPartitions(1).
				WithNumMessages(5).
				WithAcks(tc.acks).
				When().
				StartBroker().
				CreateTopic().
				PublishMessages().
				Then().
				Expect(MessagesPublished(5))
		})
	}
}

// TestIdempotencyUnderStress tests idempotency with high concurrency
func TestIdempotencyUnderStress(t *testing.T) {
	ctx := GivenStandalone(t).
		WithTopic("idempotency-stress").
		WithPartitions(5).
		WithNumMessages(1000).
		WithAcks("all").
		WithIdempotent(true).
		WithPublishDelay(0) // no delay
	defer ctx.Cleanup()

	ctx.When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		DisconnectClient().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(NoDuplicateMessages()).
		And(MessagesConsumed(1000)).
		And(OffsetsCommitted())
}
