package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"sort"
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

func ExpectReplicatedMessages(expectedPartitions, expectedReplicas, minISR int, expectedRecords uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetAcks() != "all" {
			return fmt.Errorf("replication contract requires acks=all, got %q", ctx.GetAcks())
		}
		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("published %d messages, want %d", ctx.GetPublishedCount(), ctx.GetNumMessages())
		}
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		return eventually(ctx.GetT(), fmt.Sprintf("replicated committed state for %s", topic), clusterReadyTimeout, func() (bool, string, error) {
			resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
			if err != nil {
				return false, "DESCRIBE failed", err
			}
			var meta topicMetadata
			if err := json.Unmarshal([]byte(resp), &meta); err != nil {
				return false, resp, err
			}
			if err := validateTopicMetadata(meta, topic, expectedPartitions, expectedReplicas, minISR, expectedRecords); err != nil {
				return false, err.Error(), nil
			}
			return true, fmt.Sprintf("partitions=%d replicas=%d min_isr=%d leo=hwm=%d", expectedPartitions, expectedReplicas, minISR, expectedRecords), nil
		})
	}
}

func validateTopicMetadata(meta topicMetadata, expectedTopic string, expectedPartitions, expectedReplicas, minISR int, expectedRecords uint64) error {
	if meta.Topic != expectedTopic {
		return fmt.Errorf("topic=%q, want %q", meta.Topic, expectedTopic)
	}
	if len(meta.Partitions) != expectedPartitions {
		return fmt.Errorf("partitions=%d, want %d", len(meta.Partitions), expectedPartitions)
	}
	if expectedReplicas < minISR {
		return fmt.Errorf("invalid expectation: replicas=%d min_isr=%d", expectedReplicas, minISR)
	}

	seenPartitions := make(map[int]struct{}, expectedPartitions)
	for _, partition := range meta.Partitions {
		if _, exists := seenPartitions[partition.ID]; exists {
			return fmt.Errorf("partition %d appears more than once", partition.ID)
		}
		seenPartitions[partition.ID] = struct{}{}
		if partition.Leader == "" {
			return fmt.Errorf("partition %d has no leader", partition.ID)
		}
		if err := validateReplicaMembership(partition, expectedReplicas, minISR); err != nil {
			return err
		}
		if partition.LEO != expectedRecords || partition.HWM != expectedRecords {
			return fmt.Errorf("partition %d leo=%d hwm=%d, want both %d", partition.ID, partition.LEO, partition.HWM, expectedRecords)
		}
	}
	partitionIDs := sortedPartitionIDs(meta)
	for expectedID, actualID := range partitionIDs {
		if actualID != expectedID {
			return fmt.Errorf("partition ids=%v, want contiguous ids starting at zero", partitionIDs)
		}
	}
	return nil
}

func validateReplicaMembership(partition partitionMetadata, expectedReplicas, minISR int) error {
	if len(partition.Replicas) != expectedReplicas {
		return fmt.Errorf("partition %d replicas=%v, want %d replicas", partition.ID, partition.Replicas, expectedReplicas)
	}
	if len(partition.ISR) < minISR {
		return fmt.Errorf("partition %d isr=%v, want at least %d in-sync replicas", partition.ID, partition.ISR, minISR)
	}

	replicas := make(map[string]struct{}, len(partition.Replicas))
	for _, replica := range partition.Replicas {
		if replica == "" {
			return fmt.Errorf("partition %d contains an empty replica id", partition.ID)
		}
		if _, exists := replicas[replica]; exists {
			return fmt.Errorf("partition %d contains duplicate replica %q", partition.ID, replica)
		}
		replicas[replica] = struct{}{}
	}
	seenISR := make(map[string]struct{}, len(partition.ISR))
	for _, replica := range partition.ISR {
		if _, exists := replicas[replica]; !exists {
			return fmt.Errorf("partition %d ISR member %q is not assigned", partition.ID, replica)
		}
		if _, exists := seenISR[replica]; exists {
			return fmt.Errorf("partition %d contains duplicate ISR member %q", partition.ID, replica)
		}
		seenISR[replica] = struct{}{}
	}
	return nil
}

func sortedPartitionIDs(meta topicMetadata) []int {
	ids := make([]int, 0, len(meta.Partitions))
	for _, partition := range meta.Partitions {
		ids = append(ids, partition.ID)
	}
	sort.Ints(ids)
	return ids
}
func ExpectPartitionWatermarks(partitionID int, expectedLEO, expectedHWM uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		return eventually(ctx.GetT(), fmt.Sprintf("watermarks for %s[%d]", ctx.GetTopic(), partitionID), clusterReadyTimeout, func() (bool, string, error) {
			resp, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", ctx.GetTopic()), 5*time.Second)
			if err != nil {
				return false, "DESCRIBE failed", err
			}
			var meta topicMetadata
			if err := json.Unmarshal([]byte(resp), &meta); err != nil {
				return false, resp, err
			}
			for _, partition := range meta.Partitions {
				if partition.ID == partitionID {
					return partition.LEO == expectedLEO && partition.HWM == expectedHWM, fmt.Sprintf("leo=%d hwm=%d", partition.LEO, partition.HWM), nil
				}
			}
			return false, "partition not found", nil
		})
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
