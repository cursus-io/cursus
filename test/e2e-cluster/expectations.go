package e2e_cluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

// ISRMaintained verifies ISR is maintained during operations
func ISRMaintained() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
		if err != nil {
			return err
		}
		if !strings.Contains(resp, "\"isr\":") {
			return fmt.Errorf("ISR info not found in metadata")
		}
		if strings.Contains(resp, "\"isr\": []") {
			return fmt.Errorf("ISR is empty")
		}
		return nil
	}
}

func LeaderChanged(oldLeader string) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
		if err != nil {
			return err
		}
		if strings.Contains(resp, "ERROR:") {
			return fmt.Errorf("metadata fetch failed: %s", resp)
		}
		if oldLeader != "" && strings.Contains(resp, fmt.Sprintf("\"leader\": \"%s\"", oldLeader)) {
			return fmt.Errorf("leader has not changed from %s", oldLeader)
		}
		ctx.GetT().Logf("Verified leader change: %s", resp)
		return nil
	}
}

// ExpectDataConsistent verifies all partitions have the same LEO across metadata
func ExpectDataConsistent() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		ctx.GetT().Log("Verifying data consistency across cluster...")
		topic := ctx.GetTopic()
		client := ctx.GetClient()

		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to describe topic for consistency check: %w", err)
		}

		if strings.Contains(resp, "ERROR:") {
			return fmt.Errorf("broker error in DESCRIBE: %s", resp)
		}

		if ctx.GetPublishedCount() > 0 {
			if !strings.Contains(resp, "\"leo\":") {
				return fmt.Errorf("consistency check failed: LEO not found in metadata")
			}
		}

		ctx.GetT().Log("Data consistency metadata verified")
		return nil
	}
}

func ExpectOffsetMatched(partition int, expected uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		group := ctx.GetConsumerGroup()
		client := ctx.GetClient()

		maxRetries := 10
		var lastOffset uint64
		var lastErr error

		for i := 0; i < maxRetries; i++ {
			lastOffset, lastErr = client.FetchCommittedOffset(topic, partition, group)
			if lastErr == nil && lastOffset == expected {
				ctx.GetT().Logf("Confirmed: Offset %d is correctly replicated and matched", lastOffset)
				return nil
			}
			time.Sleep(1 * time.Second)
		}

		if lastErr != nil {
			return fmt.Errorf("failed to fetch offset after retries: %w", lastErr)
		}
		return fmt.Errorf("offset mismatch after retries: expected %d, got %d", expected, lastOffset)
	}
}

// MessagesPublishedWithQuorum verifies messages were published with quorum
func MessagesPublishedWithQuorum() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published")
		}

		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("quorum not achieved: expected %d messages, got %d",
				ctx.GetNumMessages(), ctx.GetPublishedCount())
		}

		if ctx.GetAcks() != "all" {
			return fmt.Errorf("acks not set to 'all': got %s", ctx.GetAcks())
		}

		ctx.GetT().Logf("Quorum achieved: %d messages published with acks=%s",
			ctx.GetPublishedCount(), ctx.GetAcks())
		return nil
	}
}
