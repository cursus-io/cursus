package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCleanBootstrapRollingRestartRestoresFullISR(t *testing.T) {
	if os.Getenv("RUN_E2E_ROLLING_RESTART") != "1" {
		t.Skip("set RUN_E2E_ROLLING_RESTART=1 to run clean-bootstrap rolling-restart validation")
	}
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("clean-bootstrap-rolling-restart").
		WithNumMessages(30).
		WithAcks("all")

	actions := ctx.WhenCluster().StartCluster().CreateTopic()
	actions.WaitForTopicMetadata().PublishMessages()
	waitForFullISRAndZeroUnderReplicated(t, ctx, "initial publish")

	for node := 1; node <= 3; node++ {
		actions.StopBroker(node)
		actions.StartBroker(node)
		actions.WaitForTopicMetadata()
		waitForFullISRAndZeroUnderReplicated(t, ctx, fmt.Sprintf("broker-%d restart", node))
	}

	if ctx.GetPublishedCount() != ctx.GetNumMessages() {
		t.Fatalf("rolling restart published additional data: count=%d fixture=%d", ctx.GetPublishedCount(), ctx.GetNumMessages())
	}
}

func waitForFullISRAndZeroUnderReplicated(t *testing.T, ctx *ClusterTestContext, phase string) {
	t.Helper()
	err := eventually(t, "full ISR and zero under-replicated partitions after "+phase, 2*clusterReadyTimeout, func() (bool, string, error) {
		response, err := ctx.GetClient().SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", ctx.GetTopic()), 5*time.Second)
		if err != nil {
			return false, "DESCRIBE failed", nil
		}
		var metadata topicMetadata
		if err := json.Unmarshal([]byte(response), &metadata); err != nil {
			return false, response, err
		}
		if len(metadata.Partitions) == 0 {
			return false, "topic has no partitions", nil
		}
		for _, partition := range metadata.Partitions {
			if len(partition.Replicas) != 3 || len(partition.ISR) != 3 {
				return false, fmt.Sprintf("partition=%d replicas=%v isr=%v", partition.ID, partition.Replicas, partition.ISR), nil
			}
		}
		for node := 1; node <= 3; node++ {
			value, err := fetchMetric(node, "cursus_cluster_under_replicated_partitions")
			if err != nil {
				return false, fmt.Sprintf("broker-%d metric: %v", node, err), nil
			}
			if value != 0 {
				return false, fmt.Sprintf("broker-%d under_replicated=%v", node, value), nil
			}
		}
		return true, "isr=3 under_replicated=0", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
