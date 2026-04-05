package e2e

import (
	"fmt"
	"time"
)

// Consequences represents test assertions (Then phase)
type Consequences struct {
	ctx *TestContext
}

// Expectation is a function that validates test outcomes
type Expectation func(*TestContext) error

func (c *Consequences) Expect(expectations ...Expectation) *Consequences {
	for _, expectation := range expectations {
		if err := expectation(c.ctx); err != nil {
			c.ctx.t.Error(err)
		}
	}
	return c
}

func (c *Consequences) And(expectations ...Expectation) *Consequences {
	return c.Expect(expectations...)
}

// Common expectations
func BrokerIsHealthy(healthCheckURLs []string) Expectation {
	return func(ctx *TestContext) error {
		if err := CheckBrokerHealth(healthCheckURLs); err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}
		return nil
	}
}

// MessagesPublished verifies the count of published messages.
func MessagesPublished(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		return nil
	}
}

// MessagesConsumed verifies the count of consumed messages.
func MessagesConsumed(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount != expected {
			return fmt.Errorf("expected %d messages consumed, got %d", expected, ctx.consumedCount)
		}
		return nil
	}
}

// PublisherRetriedSuccessfully verifies that at least some messages were published.
func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount == 0 {
			return fmt.Errorf("no messages were published")
		}
		return nil
	}
}

// MessagesPublishedSince verifies the count and ensures publication happened within a relevant timeframe.
func MessagesPublishedSince(expected int, since time.Time) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		if ctx.startTime.Before(since) {
			return fmt.Errorf("publication started before expected time: started at %v, expected since %v", ctx.startTime, since)
		}
		return nil
	}
}

// OffsetsCommitted verifies that the offset for the partition is not 0
func OffsetsCommitted() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount == 0 {
			return fmt.Errorf("no messages consumed, cannot verify offset commit")
		}

		client := NewBrokerClient(ctx.brokerAddrs)
		defer client.Close()

		for partition := 0; partition < ctx.partitions; partition++ {
			offset, err := client.FetchCommittedOffset(ctx.topic, partition, ctx.consumerGroup)
			if err != nil {
				return fmt.Errorf("failed to fetch committed offset for partition %d: %w", partition, err)
			}
			if offset == 0 {
				return fmt.Errorf("offset not committed for partition %d", partition)
			}
		}
		return nil
	}
}

// ConsumerGroupIsActive verifies the group status via broker API.
func ConsumerGroupIsActive() Expectation {
	return func(ctx *TestContext) error {
		client := NewBrokerClient(ctx.brokerAddrs)
		defer client.Close()

		status, err := client.GetConsumerGroupStatus(ctx.consumerGroup)
		if err != nil {
			return fmt.Errorf("failed to get group status: %w", err)
		}

		if status.GroupName != ctx.consumerGroup {
			return fmt.Errorf("expected group name %s, got %s", ctx.consumerGroup, status.GroupName)
		}

		if status.MemberCount < 1 {
			return fmt.Errorf("expected at least 1 member in group %s, got %d", ctx.consumerGroup, status.MemberCount)
		}

		return nil
	}
}

func HeartbeatsSent() Expectation {
	return func(ctx *TestContext) error {
		client := NewBrokerClient(ctx.brokerAddrs)
		defer client.Close()

		status, err := client.GetConsumerGroupStatus(ctx.consumerGroup)
		if err != nil {
			return fmt.Errorf("failed to get group status for %s: %w", ctx.consumerGroup, err)
		}

		if status.GroupName != ctx.consumerGroup {
			return fmt.Errorf("expected group name %s, got %s", ctx.consumerGroup, status.GroupName)
		}

		if status.MemberCount < 1 {
			return fmt.Errorf("expected at least 1 member in group %s, got %d. (Run ConsumeMessages first)", ctx.consumerGroup, status.MemberCount)
		}

		threshold := time.Now().Add(-5 * time.Second)
		for _, member := range status.Members {
			if member.MemberID == ctx.memberID {
				if member.LastHeartbeat.Before(threshold) {
					return fmt.Errorf("member %s last heartbeat was too old: %v", member.MemberID, member.LastHeartbeat)
				}
				ctx.t.Logf("Member %s last heartbeat verified: %v", member.MemberID, member.LastHeartbeat)
				return nil
			}
		}
		return fmt.Errorf("could not find E2E client member ID (%s) in group status", ctx.memberID)
	}
}

// PublishFailed verifies that publish operations failed as expected
func PublishFailed() Expectation {
	return func(ctx *TestContext) error {
		if ctx.lastError == nil && ctx.publishedCount > 0 {
			return fmt.Errorf("expected publish to fail but it succeeded")
		}

		if ctx.publishedCount > 0 {
			return fmt.Errorf("publish failed with error (%v), but %d messages were still recorded",
				ctx.lastError, ctx.publishedCount)
		}

		ctx.GetT().Logf("Publish failed as expected with error: %v", ctx.lastError)
		return nil
	}
}

// NoDuplicateMessages verifies no duplicates in consumed messages
func NoDuplicateMessages() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount == 0 {
			return fmt.Errorf("no messages consumed to check for duplicates")
		}

		if ctx.consumedCount > ctx.publishedCount {
			return fmt.Errorf("consumed %d messages but only %d published (duplicates detected)",
				ctx.consumedCount, ctx.publishedCount)
		}

		ctx.GetT().Logf("Verified no duplicates: %d published, %d consumed",
			ctx.publishedCount, ctx.consumedCount)
		return nil
	}
}

func PublishedSequencesAreUnique() Expectation {
	return func(ctx *TestContext) error {
		if len(ctx.publishedSeqNums) == 0 {
			return fmt.Errorf("no sequence numbers tracked for duplicate detection")
		}

		seen := make(map[uint64]bool)
		for _, seq := range ctx.publishedSeqNums {
			if seen[seq] {
				return fmt.Errorf("publisher duplicate sequence number detected: %d", seq)
			}
			seen[seq] = true
		}
		ctx.GetT().Logf("Verified: All %d published sequences are unique", len(ctx.publishedSeqNums))
		return nil
	}
}

// DuplicatesDetected verifies that the consumed count is higher than published due to lack of idempotency
func DuplicatesDetected() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount <= ctx.publishedCount {
			return fmt.Errorf("expected duplicate messages (consumed > %d), but got %d (no duplicates occurred)", ctx.publishedCount, ctx.consumedCount)
		}

		ctx.t.Logf("Duplicates detected as expected: Published %d, Consumed %d", ctx.publishedCount, ctx.consumedCount)
		return nil
	}
}

func DuplicatesAllowed() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount < ctx.publishedCount {
			return fmt.Errorf("message loss detected: published %d, but only consumed %d", ctx.publishedCount, ctx.consumedCount)
		}
		ctx.t.Logf("Consumption verified (duplicates allowed): Published %d, Consumed %d", ctx.publishedCount, ctx.consumedCount)
		return nil
	}
}

func MessageLossAllowed() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount == 0 && ctx.publishedCount > 0 {
			return fmt.Errorf("all messages lost: published %d, but consumed 0", ctx.publishedCount)
		}
		ctx.t.Logf("Consumption verified (message loss allowed): Published %d, Consumed %d", ctx.publishedCount, ctx.consumedCount)
		return nil
	}
}
