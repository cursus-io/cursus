package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
)

const rollingRestartISRStabilityWindow = 6 * time.Second

func TestCleanBootstrapRollingRestartRestoresFullISR(t *testing.T) {
	if os.Getenv("RUN_E2E_ROLLING_RESTART") != "1" {
		t.Skip("set RUN_E2E_ROLLING_RESTART=1 to run clean-bootstrap rolling-restart validation")
	}
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("clean-bootstrap-rolling-restart").
		WithNumMessages(30).
		WithIdempotent(true).
		WithAcks("all")

	actions := ctx.WhenCluster().StartCluster().CreateTopic()
	actions.WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "topic creation")
	actions.PublishMessages()
	if err := ctx.GetLastError(); err != nil {
		t.Fatalf("rolling restart fixture publish failed: %v", err)
	}
	if ctx.GetPublishedCount() != ctx.GetNumMessages() {
		t.Fatalf("rolling restart fixture is incomplete: published=%d want=%d", ctx.GetPublishedCount(), ctx.GetNumMessages())
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "initial publish")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), uint64(ctx.GetNumMessages()))

	for node := 1; node <= ctx.clusterSize; node++ {
		actions.StopBroker(node)
		waitForBrokerEvictedFromISR(t, ctx, node)
		actions.StartBroker(node)
		actions.WaitForTopicMetadata()
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, fmt.Sprintf("broker-%d restart", node))
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), uint64(ctx.GetNumMessages()))
	}

	verifyLiveConsumerOffsetCatchup(t, ctx, actions)

	// Lose quorum while preserving each broker's durable volume. Membership can
	// become active again before a restarted replica has replayed its committed
	// range, so readiness and the ISR-backed catch-up path must converge data
	// before this phase can complete.
	actions.StopBroker(1)
	actions.StopBroker(2)
	actions.StartBroker(1)
	actions.StartBroker(2)
	actions.WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "two-broker quorum-loss restart")
	requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), uint64(ctx.GetNumMessages()))
}

func verifyLiveConsumerOffsetCatchup(t *testing.T, ctx *ClusterTestContext, actions *ClusterActions) {
	t.Helper()
	const group = "rolling-restart-live-offsets"
	client, generation, member := joinClusterGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
	defer client.Close()
	commitGroupOffset(t, client, ctx.GetTopic(), group, 1, generation, member)
	offsetPartition, _ := consumerOffsetPartition(t, ctx.GetBrokerAddrs(), group)
	coordinator := coordinatorNode(t, findGroupCoordinator(t, 1, group))
	recoveryNode := 1
	if recoveryNode == coordinator {
		recoveryNode = 2
	}

	actions.StopBroker(recoveryNode)
	waitForBrokerEvictedFromPartitionISR(t, ctx, config.ConsumerOffsetsTopicName, offsetPartition, recoveryNode)
	for offset := uint64(2); offset <= 64; offset++ {
		commitGroupOffset(t, client, ctx.GetTopic(), group, offset, generation, member)
		heartbeatGroup(t, client, ctx.GetTopic(), group, generation, member)
	}
	actions.StartBroker(recoveryNode)
	actions.WaitForTopicMetadata()

	deadline := time.Now().Add(2 * clusterReadyTimeout)
	var stableSince time.Time
	latestOffset := uint64(64)
	for time.Now().Before(deadline) {
		latestOffset++
		commitGroupOffset(t, client, ctx.GetTopic(), group, latestOffset, generation, member)
		heartbeatGroup(t, client, ctx.GetTopic(), group, generation, member)
		ready, _, err := partitionFullISRAndZeroUnderReplicated(ctx, config.ConsumerOffsetsTopicName, offsetPartition)
		if err == nil && ready {
			if stableSince.IsZero() {
				stableSince = time.Now()
			} else if time.Since(stableSince) >= rollingRestartISRStabilityWindow {
				requireReplicaPartitionOffsetsConverge(t, ctx.GetBrokerAddrs(), config.ConsumerOffsetsTopicName, offsetPartition)
				if committed := fetchCommittedOffset(t, client, ctx.GetTopic(), group); committed != latestOffset {
					t.Fatalf("live committed offset=%d want=%d", committed, latestOffset)
				}
				return
			}
		} else {
			stableSince = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("consumer-offset partition %d did not restore full ISR while commits continued", offsetPartition)
}

func waitForBrokerEvictedFromPartitionISR(t *testing.T, ctx *ClusterTestContext, topicName string, partitionID, node int) {
	t.Helper()
	brokerID := fmt.Sprintf("broker-%d-%d", node, baseBrokerPort)
	err := eventually(t, fmt.Sprintf("%s eviction from %s-%d ISR", brokerID, topicName, partitionID), clusterReadyTimeout, func() (bool, string, error) {
		response, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topicName), 5*time.Second)
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		var metadata topicMetadata
		if err := json.Unmarshal([]byte(response), &metadata); err != nil {
			return false, response, err
		}
		for _, partition := range metadata.Partitions {
			if partition.ID != partitionID {
				continue
			}
			for _, member := range partition.ISR {
				if member == brokerID {
					return false, fmt.Sprintf("broker=%s still present in isr=%v", brokerID, partition.ISR), nil
				}
			}
			return len(partition.ISR) == ctx.clusterSize-1, fmt.Sprintf("partition=%d isr=%v", partitionID, partition.ISR), nil
		}
		return false, fmt.Sprintf("partition=%d missing", partitionID), nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func partitionFullISRAndZeroUnderReplicated(ctx *ClusterTestContext, topicName string, partitionID int) (bool, string, error) {
	response, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topicName), 5*time.Second)
	if err != nil {
		return false, "DESCRIBE failed", err
	}
	var metadata topicMetadata
	if err := json.Unmarshal([]byte(response), &metadata); err != nil {
		return false, response, err
	}
	for _, partition := range metadata.Partitions {
		if partition.ID != partitionID {
			continue
		}
		if len(partition.Replicas) != ctx.clusterSize || len(partition.ISR) != ctx.clusterSize {
			return false, fmt.Sprintf("partition=%d replicas=%v isr=%v", partitionID, partition.Replicas, partition.ISR), nil
		}
		for node := 1; node <= ctx.clusterSize; node++ {
			value, metricErr := fetchMetric(node, "cursus_cluster_under_replicated_partitions")
			if metricErr != nil || value != 0 {
				return false, fmt.Sprintf("broker-%d under_replicated=%v err=%v", node, value, metricErr), nil
			}
		}
		return true, fmt.Sprintf("partition=%d full ISR", partitionID), nil
	}
	return false, fmt.Sprintf("partition=%d missing", partitionID), nil
}

func waitForBrokerEvictedFromISR(t *testing.T, ctx *ClusterTestContext, node int) {
	t.Helper()
	brokerID := fmt.Sprintf("broker-%d-%d", node, baseBrokerPort)
	err := eventually(t, fmt.Sprintf("%s eviction from ISR", brokerID), clusterReadyTimeout, func() (bool, string, error) {
		response, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", ctx.GetTopic()), 5*time.Second)
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		var metadata topicMetadata
		if err := json.Unmarshal([]byte(response), &metadata); err != nil {
			return false, response, err
		}
		ready, detail := brokerEvictedFromISR(metadata, brokerID, ctx.clusterSize)
		return ready, detail, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func brokerEvictedFromISR(metadata topicMetadata, brokerID string, clusterSize int) (bool, string) {
	if clusterSize < 2 {
		return false, fmt.Sprintf("invalid cluster size %d", clusterSize)
	}
	if len(metadata.Partitions) == 0 {
		return false, "topic has no partitions"
	}
	for _, partition := range metadata.Partitions {
		if len(partition.Replicas) != clusterSize {
			return false, fmt.Sprintf("partition=%d replicas=%v want=%d", partition.ID, partition.Replicas, clusterSize)
		}
		for _, member := range partition.ISR {
			if member == brokerID {
				return false, fmt.Sprintf("partition=%d broker=%s still present in isr=%v", partition.ID, brokerID, partition.ISR)
			}
		}
		if len(partition.ISR) != clusterSize-1 {
			return false, fmt.Sprintf("partition=%d isr=%v want=%d remaining replicas", partition.ID, partition.ISR, clusterSize-1)
		}
	}
	return true, fmt.Sprintf("broker=%s absent from ISR across %d partitions", brokerID, len(metadata.Partitions))
}

func waitForFullISRAndZeroUnderReplicated(t *testing.T, ctx *ClusterTestContext, phase string) {
	t.Helper()
	err := eventually(t, "full ISR and zero under-replicated partitions after "+phase, 2*clusterReadyTimeout, func() (bool, string, error) {
		return fullISRAndZeroUnderReplicated(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitForStableFullISRAndZeroUnderReplicated(t *testing.T, ctx *ClusterTestContext, phase string) {
	t.Helper()
	err := eventuallyStable(
		t,
		"stable full ISR and zero under-replicated partitions after "+phase,
		2*clusterReadyTimeout,
		rollingRestartISRStabilityWindow,
		func() (bool, string, error) { return fullISRAndZeroUnderReplicated(ctx) },
	)
	if err != nil {
		t.Fatal(err)
	}
}

func fullISRAndZeroUnderReplicated(ctx *ClusterTestContext) (bool, string, error) {
	response, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", ctx.GetTopic()), 5*time.Second)
	if err != nil {
		return false, "DESCRIBE failed", err
	}
	var metadata topicMetadata
	if err := json.Unmarshal([]byte(response), &metadata); err != nil {
		return false, response, err
	}
	if len(metadata.Partitions) == 0 {
		return false, "topic has no partitions", nil
	}
	for _, partition := range metadata.Partitions {
		if len(partition.Replicas) != ctx.clusterSize || len(partition.ISR) != ctx.clusterSize {
			return false, fmt.Sprintf("partition=%d replicas=%v isr=%v", partition.ID, partition.Replicas, partition.ISR), nil
		}
	}
	for node := 1; node <= ctx.clusterSize; node++ {
		value, err := fetchMetric(node, "cursus_cluster_under_replicated_partitions")
		if err != nil {
			return false, fmt.Sprintf("broker-%d metric: %v", node, err), nil
		}
		if value != 0 {
			return false, fmt.Sprintf("broker-%d under_replicated=%v", node, value), nil
		}
	}
	return true, fmt.Sprintf("isr=%d under_replicated=0", ctx.clusterSize), nil
}
