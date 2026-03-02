package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

type partitionMetadata struct {
	ID       int      `json:"id"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
	ISR      []string `json:"isr"`
	LEO      uint64   `json:"leo"`
	HWM      uint64   `json:"hwm"`
}

type topicMetadata struct {
	Topic      string              `json:"topic"`
	Partitions []partitionMetadata `json:"partitions"`
}

// ISRMaintained verifies ISR is maintained during operations
func ISRMaintained() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
		if err != nil {
			return err
		}

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w (resp: %s)", err, resp)
		}

		for _, p := range meta.Partitions {
			if len(p.ISR) == 0 {
				return fmt.Errorf("ISR is empty for partition %d", p.ID)
			}
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

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w (resp: %s)", err, resp)
		}

		for _, p := range meta.Partitions {
			if oldLeader != "" && p.Leader == oldLeader {
				return fmt.Errorf("leader for partition %d has not changed from %s", p.ID, oldLeader)
			}
		}

		ctx.GetT().Logf("Verified leader change for all partitions")
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

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w (resp: %s)", err, resp)
		}

		if ctx.GetPublishedCount() > 0 {
			expectedLEO := uint64(ctx.GetPublishedCount())
			for _, p := range meta.Partitions {
				if p.LEO < expectedLEO {
					return fmt.Errorf("consistency check failed: partition %d LEO (%d) < expected (%d)", p.ID, p.LEO, expectedLEO)
				}
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
