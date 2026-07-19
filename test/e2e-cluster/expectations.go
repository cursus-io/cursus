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
				ctx.GetT().Logf("DEBUG: ISR empty for partition %d. Metadata: %+v", p.ID, p)
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

func ExpectPartitionWatermarks(partition int, expectedLEO, expectedHWM uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		resp, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", ctx.GetTopic()), 5*time.Second)
		if err != nil {
			return err
		}

		var meta topicMetadata
		if err := json.Unmarshal([]byte(resp), &meta); err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}
		for _, p := range meta.Partitions {
			if p.ID != partition {
				continue
			}
			if p.LEO != expectedLEO || p.HWM != expectedHWM {
				return fmt.Errorf("partition %d watermark mismatch: expected leo=%d hwm=%d, got leo=%d hwm=%d",
					partition, expectedLEO, expectedHWM, p.LEO, p.HWM)
			}
			return nil
		}
		return fmt.Errorf("partition %d not found", partition)
	}
}

func ExpectOffsetMatched(partition int, expected uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		group := ctx.GetConsumerGroup()
		client := ctx.GetClient()
		return eventually(ctx.GetT(), fmt.Sprintf("committed offset %d for %s[%d]", expected, topic, partition), clusterReadyTimeout, func() (bool, string, error) {
			offset, err := client.FetchCommittedOffset(topic, partition, group)
			if err != nil {
				return false, "fetch committed offset failed", err
			}
			return offset == expected, fmt.Sprintf("got %d, want %d", offset, expected), nil
		})
	}
}
func LeaderChanged(partitionID int, oldLeader string) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		return eventually(ctx.GetT(), fmt.Sprintf("leader change for %s[%d]", topic, partitionID), clusterReadyTimeout, func() (bool, string, error) {
			resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
			if err != nil {
				return false, "DESCRIBE failed", err
			}
			var meta topicMetadata
			if err := json.Unmarshal([]byte(resp), &meta); err != nil {
				return false, resp, err
			}
			for _, partition := range meta.Partitions {
				if partition.ID == partitionID {
					return partition.Leader != "" && partition.Leader != oldLeader, fmt.Sprintf("leader=%s", partition.Leader), nil
				}
			}
			return false, "partition not found", nil
		})
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

		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("expected %d messages to be published with quorum, but got %d",
				ctx.GetNumMessages(), ctx.GetPublishedCount())
		}

		ctx.GetT().Logf("Quorum achieved: %d messages published with acks=%s",
			ctx.GetPublishedCount(), ctx.GetAcks())
		return nil
	}
}
