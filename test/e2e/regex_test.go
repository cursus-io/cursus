package e2e

import (
	"fmt"
	"testing"
	"time"
 
	"github.com/google/uuid"
)

// TestMultipleTopicConsumption tests consuming from multiple topics
func TestMultipleTopicConsumption(t *testing.T) {
	uniqueID := uuid.New().String()[:4]
	prefix := "mult-" + uniqueID
	topics := []string{prefix + "-a", prefix + "-b", prefix + "-c", "other-topic-" + uniqueID}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	totalConsumed := 0
	matchingTopics := []string{prefix + "-a", prefix + "-b", prefix + "-c"}

	for i, topic := range matchingTopics {
		groupName := fmt.Sprintf("mult-%s-group-%d", uniqueID, i)

		consumerCtx := GivenStandalone(t).
			WithTopic(topic).
			WithPartitions(1).
			WithNumMessages(0).
			WithConsumerGroup(groupName)
		defer consumerCtx.Cleanup()

		consumerCtx.When().
			JoinGroup().
			SyncGroup().
			ConsumeMessages().
			Then().
			Expect(MessagesConsumed(5))
		totalConsumed += 5
	}

	if totalConsumed != 15 {
		t.Errorf("Expected 15 messages consumed, got %d", totalConsumed)
	}
}

// TestRegexPatternConsumption tests actual regex pattern matching
func TestRegexPatternConsumption(t *testing.T) {
	uniqueID := uuid.New().String()[:4]
	prefix := "regx-" + uniqueID
	topics := []string{prefix + "-a", prefix + "-b", prefix + "-c", prefix + "-topic"}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	groupName := "regx-" + uniqueID + "-group"
	consumerCtx := GivenStandalone(t).
		WithTopic(prefix + "-a"). // actual topic name for group registration
		WithPartitions(1).
		WithNumMessages(0).
		WithConsumerGroup(groupName)
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessagesFromTopic(prefix + "-*"). // regex pattern consume matches alpha, beta, gamma, topic (4 topics * 5 msgs)
		Then().
		Expect(MessagesConsumed(20))
}

// TestRegexPatternWithQuestionMark tests ? wildcard functionality
func TestRegexPatternWithQuestionMark(t *testing.T) {
	uniqueID := uuid.New().String()[:4]
	prefix := "qmrk-" + uniqueID
	topics := []string{prefix + "-1", prefix + "-2", prefix + "-3"}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(3)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	groupName := "qmrk-" + uniqueID + "-group"
	consumerCtx := GivenStandalone(t).
		WithTopic(prefix + "-1"). // actual topic name
		WithPartitions(1).
		WithNumMessages(0).
		WithConsumerGroup(groupName)
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessagesFromTopic(prefix + "-?"). // matches log-1, log-2, log-3 (3 topics * 3 msgs)
		Then().
		Expect(MessagesConsumed(9))
}
