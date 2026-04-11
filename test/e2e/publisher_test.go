package e2e

import (
	"testing"
)

// TestPublisherConfigOptions verifies publisher configuration options work correctly
func TestPublisherConfigOptions(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("publisher-test").
		WithPartitions(2).
		WithNumMessages(5).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		Then().
		Expect(BrokerIsHealthy(StandAloneHealthCheckAddr)).
		And(MessagesPublished(5))
}

// TestPublisherRetryLogic verifies publisher retry mechanism
func TestPublisherRetryLogic(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("retry-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		StopBroker().
		StartBroker().
		CreateTopic().
		PublishMessages().
		Then().
		Expect(PublisherRetriedSuccessfully())
}

// TestExactlyOnceSemantics verifies exactly-once delivery with retries
func TestExactlyOnceSemantics(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("exactly-once-test").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

// TestIdempotentProducer verifies idempotent producer behavior
func TestIdempotentProducer(t *testing.T) {
	ctx := GivenStandalone(t)
	defer ctx.Cleanup()

	ctx.WithTopic("idempotent-test").
		WithPartitions(1).
		WithNumMessages(5).
		WithIdempotent(true).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		SimulateNetworkFailure().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(5)).
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

// TestExactlyOnceWithFailures tests exactly-once semantics with various failure scenarios
func TestExactlyOnceWithFailures(t *testing.T) {
	testCases := []struct {
		name        string
		acks        string
		failures    []string
		expectDupes bool
		expectLoss  bool
	}{
		{"acks=1_network_failure", "1", []string{"network"}, false, false},
		{"acks=all_broker_failure", "all", []string{"broker"}, false, false},
		{"acks=0_no_guarantee", "0", []string{"network"}, true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := GivenStandalone(t).
				WithTopic("exactly-once-" + tc.name).
				WithPartitions(3).
				WithNumMessages(50).
				WithAcks(tc.acks)
			defer ctx.Cleanup()

			actions := ctx.When().
				StartBroker().
				CreateTopic().
				PublishMessages()

			for _, failure := range tc.failures {
				switch failure {
				case "network":
					actions.SimulateNetworkFailure()
				case "broker":
					actions.StopBroker().StartBroker()
				}
			}

			consequences := actions.RetryPublishMessages().
				JoinGroup().
				SyncGroup().
				ConsumeMessages().
				Then()

			if tc.expectLoss {
				consequences.Expect(MessageLossAllowed()).
					And(PublisherRetriedSuccessfully())
			} else if tc.expectDupes {
				consequences.Expect(DuplicatesAllowed()).
					And(PublisherRetriedSuccessfully())
			} else {
				consequences.Expect(NoDuplicateMessages()).
					And(MessagesConsumed(50))
			}
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
		WithPublishDelay(0) // no delay
	defer ctx.Cleanup()

	ctx.When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		SimulateNetworkFailure().
		RetryPublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(NoDuplicateMessages()).
		And(MessagesConsumed(1000)).
		And(OffsetsCommitted())
}
