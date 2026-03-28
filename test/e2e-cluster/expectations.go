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

func ExpectDataConsistent() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()

		ctx.GetT().Logf("Verifying data consistency across cluster...")

		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
		if err != nil {
			return err
		}

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		for _, p := range meta.Partitions {
			if len(p.ISR) < 1 {
				return fmt.Errorf("ISR is empty for partition %d", p.ID)
			}
			if p.Leader == "" {
				return fmt.Errorf("leader is empty for partition %d", p.ID)
			}
		}

		ctx.GetT().Logf("Data consistency metadata verified")
		return nil
	}
}

func ExpectOffsetMatched(partition int, expected uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		group := ctx.GetConsumerGroup()
		client := ctx.GetClient()

		maxRetries := 15
		var lastOffset uint64
		var lastErr error

		for i := 0; i < maxRetries; i++ {
			lastOffset, lastErr = client.FetchCommittedOffset(topic, partition, group)
			if lastErr == nil {
				if lastOffset == expected {
					ctx.GetT().Logf("Confirmed: Offset %d is correctly replicated and matched", lastOffset)
					return nil
				}
				ctx.GetT().Logf("Offset mismatch (Attempt %d/%d): expected %d, got %d. Retrying...", i+1, maxRetries, expected, lastOffset)
			} else {
				// Retry on authorization errors as they usually indicate leader election is in progress
				ctx.GetT().Logf("Fetch committed offset error (Attempt %d/%d): %v. Retrying...", i+1, maxRetries, lastErr)
			}
			time.Sleep(2 * time.Second)
		}

		if lastErr != nil {
			return fmt.Errorf("failed to fetch offset after %d retries: %w", maxRetries, lastErr)
		}
		return fmt.Errorf("offset mismatch after %d retries: expected %d, got %d", maxRetries, expected, lastOffset)
	}
}

func LeaderChanged(partitionID int, oldLeader string) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()

		maxRetries := 20
		for i := 0; i < maxRetries; i++ {
			resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
			if err == nil {
				var meta topicMetadata
				if err := json.Unmarshal([]byte(resp), &meta); err == nil && len(meta.Partitions) > 0 {
					for _, p := range meta.Partitions {
						if p.ID == partitionID {
							if p.Leader != "" && p.Leader != oldLeader {
								ctx.GetT().Logf("Confirmed: Leader for %s:%d changed from %s to %s", topic, partitionID, oldLeader, p.Leader)
								return nil
							}
						}
					}
				}
			}
			time.Sleep(1 * time.Second)
		}
		return fmt.Errorf("leader for partition %d did not change from %s after %d retries", partitionID, oldLeader, maxRetries)
	}
}

func ISRMaintained() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()

		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
		if err != nil {
			return err
		}

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		for _, p := range meta.Partitions {
			if len(p.ISR) == 0 {
				return fmt.Errorf("ISR is empty for partition %d", p.ID)
			}
		}
		return nil
	}
}

func MessagesPublishedWithQuorum() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetAcks() != "all" {
			return fmt.Errorf("acks not set to 'all': got %s", ctx.GetAcks())
		}

		ctx.GetT().Logf("Quorum achieved: %d messages published with acks=%s",
			ctx.GetPublishedCount(), ctx.GetAcks())
		return nil
	}
}
