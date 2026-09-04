package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
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
