package e2e_cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
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

func ExpectReplicatedMessages(expectedPartitions, expectedReplicas, expectedISR, minISR int, expectedRecords uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetAcks() != "all" {
			return fmt.Errorf("replication contract requires acks=all, got %q", ctx.GetAcks())
		}
		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("published %d messages, want %d", ctx.GetPublishedCount(), ctx.GetNumMessages())
		}
		return ExpectTopicReplication(expectedPartitions, expectedReplicas, expectedISR, minISR, expectedRecords)(ctx)
	}
}

func ExpectTopicReplication(expectedPartitions, expectedReplicas, expectedISR, minISR int, expectedRecords uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if err := validateISRContract(expectedReplicas, expectedISR, minISR, false); err != nil {
			return err
		}
		return expectTopicMetadata(ctx, expectedPartitions, expectedReplicas, expectedISR, expectedRecords)
	}
}

func ExpectISRBelowMinISR(expectedPartitions, expectedReplicas, expectedISR, minISR int, expectedRecords uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if err := validateISRContract(expectedReplicas, expectedISR, minISR, true); err != nil {
			return err
		}
		return expectTopicMetadata(ctx, expectedPartitions, expectedReplicas, expectedISR, expectedRecords)
	}
}

func PublishRejectedBelowMinISR(minISR int) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() != 0 {
			return fmt.Errorf("published %d messages below minISR %d, want 0", ctx.GetPublishedCount(), minISR)
		}
		err := ctx.GetLastError()
		if err == nil {
			return fmt.Errorf("publish below minISR %d succeeded", minISR)
		}
		if err := validateMinISRRejection(err, minISR); err != nil {
			return err
		}
		return nil
	}
}

func validateMinISRRejection(err error, minISR int) error {
	var brokerErr *wire.BrokerError
	if errors.As(err, &brokerErr) {
		if brokerErr.Code != "insufficient_in_sync_replicas" {
			return fmt.Errorf("publish failed with broker code %q, want insufficient_in_sync_replicas", brokerErr.Code)
		}
		wantRequired := strconv.Itoa(minISR)
		if brokerErr.Fields["required"] != wantRequired {
			return fmt.Errorf("insufficient_in_sync_replicas required=%q, want %s", brokerErr.Fields["required"], wantRequired)
		}
		return nil
	}

	message := err.Error()
	legacyControllerError := strings.Contains(message, "insufficient_in_sync_replicas") &&
		strings.Contains(message, fmt.Sprintf("required=%d", minISR))
	legacyReplicationError := strings.Contains(message, "insufficient") &&
		strings.Contains(message, fmt.Sprintf("minISR %d", minISR))
	if !legacyControllerError && !legacyReplicationError {
		return fmt.Errorf("publish failed with %q, want insufficient_in_sync_replicas required=%d", message, minISR)
	}
	return nil
}

func expectTopicMetadata(ctx *e2e.TestContext, expectedPartitions, expectedReplicas, expectedISR int, expectedRecords uint64) error {
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
		if err := validateTopicMetadata(meta, topic, expectedPartitions, expectedReplicas, expectedISR, expectedRecords); err != nil {
			return false, err.Error(), nil
		}
		return true, fmt.Sprintf("partitions=%d replicas=%d isr=%d leo=hwm=%d", expectedPartitions, expectedReplicas, expectedISR, expectedRecords), nil
	})
}

func validateISRContract(expectedReplicas, expectedISR, minISR int, expectBelowMinISR bool) error {
	if expectedReplicas <= 0 || expectedISR < 0 || minISR <= 0 || expectedISR > expectedReplicas || minISR > expectedReplicas {
		return fmt.Errorf("invalid replication expectation: replicas=%d isr=%d min_isr=%d", expectedReplicas, expectedISR, minISR)
	}
	if expectBelowMinISR && expectedISR >= minISR {
		return fmt.Errorf("invalid below-minISR expectation: isr=%d min_isr=%d", expectedISR, minISR)
	}
	if !expectBelowMinISR && expectedISR < minISR {
		return fmt.Errorf("unsafe replication expectation: isr=%d min_isr=%d", expectedISR, minISR)
	}
	return nil
}

func validateTopicMetadata(meta topicMetadata, expectedTopic string, expectedPartitions, expectedReplicas, expectedISR int, expectedRecords uint64) error {
	if meta.Topic != expectedTopic {
		return fmt.Errorf("topic=%q, want %q", meta.Topic, expectedTopic)
	}
	if len(meta.Partitions) != expectedPartitions {
		return fmt.Errorf("partitions=%d, want %d", len(meta.Partitions), expectedPartitions)
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
		if err := validateReplicaMembership(partition, expectedReplicas, expectedISR); err != nil {
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

func validateReplicaMembership(partition partitionMetadata, expectedReplicas, expectedISR int) error {
	if len(partition.Replicas) != expectedReplicas {
		return fmt.Errorf("partition %d replicas=%v, want %d replicas", partition.ID, partition.Replicas, expectedReplicas)
	}
	if len(partition.ISR) != expectedISR {
		return fmt.Errorf("partition %d isr=%v, want exactly %d in-sync replicas", partition.ID, partition.ISR, expectedISR)
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
