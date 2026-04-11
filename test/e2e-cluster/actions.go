package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/cursus-io/cursus/util"
)

// ClusterActions represents cluster-specific test actions
type ClusterActions struct {
	ctx     *ClusterTestContext
	actions *e2e.Actions
}

func (c *ClusterTestContext) WhenCluster() *ClusterActions {
	base := c.When()
	return &ClusterActions{
		ctx:     c,
		actions: base,
	}
}

func (a *ClusterActions) StartCluster() *ClusterActions {
	a.ctx.GetT().Logf("Waiting for %d cluster nodes to register...", a.ctx.clusterSize)

	maxRetries := 20
	for i := 0; i < maxRetries; i++ {
		resp, err := a.actions.SendCommand("LIST_CLUSTER")
		if err == nil {
			a.ctx.GetT().Logf("DEBUG: LIST_CLUSTER raw response: %s", resp)
			var brokers []fsm.BrokerInfo
			if err := json.Unmarshal([]byte(resp), &brokers); err == nil {
				activeCount := 0
				for _, b := range brokers {
					if b.Status == "active" {
						activeCount++
					}
				}
				a.ctx.GetT().Logf("LIST_CLUSTER (Attempt %d/%d): %d/%d active. Brokers: %+v", i+1, maxRetries, activeCount, a.ctx.clusterSize, brokers)
				if activeCount >= a.ctx.clusterSize {
					a.ctx.GetT().Logf("All %d cluster nodes registered and active", a.ctx.clusterSize)
					return a
				}
			}
		}
		time.Sleep(1 * time.Second)
	}

	a.ctx.GetT().Log("Cluster sync verified (with potential timeout)")
	return a
}

// waitForNodeHealth checks if a node is healthy
func (a *ClusterActions) waitForNodeHealth(nodeIndex int, healthUrl string) error {
	a.ctx.GetT().Logf("Waiting for node %d health check", nodeIndex)

	for retry := 0; retry < 30; retry++ {
		resp, err := http.Get(healthUrl)
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			a.ctx.GetT().Logf("Node %d is healthy", nodeIndex)
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("node %d failed to become healthy at %s after 30 retries", nodeIndex, healthUrl)
}

// checkAllNodesHealth verifies all cluster nodes are healthy
func (a *ClusterActions) checkAllNodesHealth() error {
	healthAddrs := clusterHealthCheckAddrs(a.ctx.clusterSize)

	for i, addr := range healthAddrs {
		if err := a.waitForNodeHealth(i+1, addr); err != nil {
			return err
		}
	}
	return nil
}

func (a *ClusterActions) CreateTopic() *ClusterActions {
	a.actions.CreateTopic()
	return a
}

func (a *ClusterActions) PublishMessages() *ClusterActions {
	a.actions.PublishMessages()
	return a
}

func (a *ClusterActions) ResetPublishedCount() *ClusterActions {
	a.ctx.ResetPublishedCount()
	return a
}

func (a *ClusterActions) RetryPublishMessages() *ClusterActions {
	a.actions.RetryPublishMessages()
	return a
}

func (a *ClusterActions) JoinGroup() *ClusterActions {
	a.actions.JoinGroup()
	return a
}

func (a *ClusterActions) SyncGroup() *ClusterActions {
	a.actions.SyncGroup()
	return a
}

func (a *ClusterActions) ConsumeMessages() *ClusterActions {
	a.actions.ConsumeMessages()
	return a
}

func (a *ClusterActions) CommitOffset(partition int, offset uint64) *ClusterActions {
	a.actions.CommitOffset(partition, offset)
	return a
}

func (a *ClusterActions) Then() *e2e.Consequences {
	return a.actions.Then()
}

func (a *ClusterActions) SimulateFollowerFailure(nodeIndex int) *ClusterActions {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("Invalid nodeIndex %d for failure simulation: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}

	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	a.ctx.GetT().Log("Simulating follower failure")

	cmd := exec.Command("docker", "stop", containerName)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to stop follower: %v", err)
		return a
	}

	a.ctx.GetClient().Close()

	a.ctx.GetT().Logf("Successfully stopped %s", containerName)
	time.Sleep(2 * time.Second)
	return a
}

func (a *ClusterActions) RecoverFollower(nodeIndex int) *ClusterActions {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("Invalid nodeIndex %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}

	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	a.ctx.GetT().Log("Recovering follower")

	cmd := exec.Command("docker", "start", containerName)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to recover follower: %v", err)
	}

	a.ctx.GetClient().Close()

	healthAddrs := clusterHealthCheckAddrs(a.ctx.clusterSize)
	if err := a.waitForNodeHealth(nodeIndex, healthAddrs[nodeIndex-1]); err != nil {
		a.ctx.GetT().Fatalf("node health check failed: %v", err)
	}
	return a
}

func (a *ClusterActions) DescribeTopic() *ClusterActions {
	topic := a.ctx.GetTopic()
	a.ctx.GetT().Logf("Describing topic: %s", topic)

	cmd := fmt.Sprintf("DESCRIBE topic=%s", topic)
	resp, err := a.actions.SendCommand(cmd)
	if err != nil {
		a.ctx.GetT().Fatalf("Failed to describe topic %s: %v", topic, err)
	}

	a.ctx.GetT().Logf("Topic Metadata:\n%s", resp)
	return a
}

func (a *ClusterActions) WaitForTopicMetadata() *ClusterActions {
	topic := a.ctx.GetTopic()
	a.ctx.GetT().Logf("Waiting for topic metadata to propagate: %s", topic)

	addrs := a.ctx.GetBrokerAddrs()

	for i := 0; i < 60; i++ {
		for _, addr := range addrs {
			tempClient := e2e.NewBrokerClient([]string{addr})
			cmd := fmt.Sprintf("DESCRIBE topic=%s", topic)
			resp, err := tempClient.SendCommand("", cmd, 2*time.Second)
			tempClient.Close()

			if err == nil &&
				!strings.Contains(resp, "not found") &&
				!strings.Contains(resp, "ERROR:") &&
				strings.Contains(resp, topic) &&
				strings.Contains(resp, "{") {
				a.ctx.GetT().Logf("Topic metadata for '%s' is now available on node %s", topic, addr)
				return a
			}
			util.Debug("Still waiting for metadata on %s: resp=%s, err=%v", addr, resp, err)
		}
		time.Sleep(1 * time.Second)
	}

	a.ctx.GetT().Fatalf("Timed out waiting for topic metadata to propagate for topic %s", topic)
	return a
}

func (a *ClusterActions) SimulateLeaderFailure() (int, *ClusterActions) {
	topic := a.ctx.GetTopic()
	a.ctx.GetT().Logf("Simulating leader failure for topic: %s", topic)

	cmd := fmt.Sprintf("DESCRIBE topic=%s", topic)
	resp, err := a.actions.SendCommand(cmd)
	if err != nil {
		a.ctx.GetT().Fatalf("Failed to find leader for topic %s: %v", topic, err)
	}

	var leaderNode int
	// "leader": "broker-X:9000"
	for i := 1; i <= a.ctx.clusterSize; i++ {
		target := fmt.Sprintf("\"leader\": \"broker-%d:9000\"", i)
		if strings.Contains(resp, target) {
			leaderNode = i
			break
		}
	}
	if leaderNode == 0 {
		for i := 1; i <= a.ctx.clusterSize; i++ {
			if strings.Contains(resp, fmt.Sprintf("broker-%d", i)) {
				leaderNode = i
				break
			}
		}
	}

	if leaderNode == 0 {
		a.ctx.GetT().Fatalf("Could not identify leader for topic %s from response: %s", topic, resp)
	}

	containerName := fmt.Sprintf("broker-%d", leaderNode)
	a.ctx.GetT().Logf("Stopping leader container: %s", containerName)

	stopCmd := exec.Command("docker", "stop", containerName)
	if err := stopCmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to stop leader %s: %v", containerName, err)
	}

	a.ctx.GetClient().Close()

	time.Sleep(5 * time.Second)
	return leaderNode, a
}
