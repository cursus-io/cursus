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
	if err := eventually(a.ctx.GetT(), "active cluster membership", clusterReadyTimeout, func() (bool, string, error) {
		resp, err := a.actions.SendCommand("LIST_CLUSTER")
		if err != nil {
			return false, "LIST_CLUSTER failed", err
		}
		payload := strings.TrimPrefix(strings.TrimSpace(resp), "OK brokers=")
		var brokers []fsm.BrokerInfo
		if err := json.Unmarshal([]byte(payload), &brokers); err != nil {
			return false, resp, err
		}
		activeCount := 0
		for _, broker := range brokers {
			if broker.Status == "active" {
				activeCount++
			}
		}
		return activeCount >= a.ctx.clusterSize, fmt.Sprintf("%d/%d active", activeCount, a.ctx.clusterSize), nil
	}); err != nil {
		a.ctx.GetT().Fatal(err)
	}
	return a
}

// waitForNodeHealth checks if a node is healthy
func (a *ClusterActions) waitForNodeHealth(nodeIndex int, healthURL string) error {
	if err := validateNodeHealthURL(nodeIndex, healthURL); err != nil {
		return err
	}
	a.ctx.GetT().Logf("Waiting for node %d health check", nodeIndex)
	return eventually(a.ctx.GetT(), fmt.Sprintf("node %d health at %s", nodeIndex, healthURL), clusterReadyTimeout, func() (bool, string, error) {
		// #nosec G107 -- validateNodeHealthURL restricts requests to the expected loopback test endpoint.
		resp, err := http.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return true, "healthy", nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		return false, "health endpoint not ready", err
	})
}

func validateNodeHealthURL(nodeIndex int, healthURL string) error {
	if nodeIndex <= 0 {
		return fmt.Errorf("invalid broker index %d", nodeIndex)
	}
	expected := fmt.Sprintf("http://localhost:%d/health", healthPort(nodeIndex))
	if healthURL != expected {
		return fmt.Errorf("refusing unexpected node health URL %q", healthURL)
	}
	return nil
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

	// #nosec G204 -- nodeIndex is range-checked above and forms a fixed broker-N container name.
	cmd := exec.Command("docker", "stop", containerName)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to stop follower: %v", err)
		return a
	}

	a.ctx.GetClient().Close()

	a.ctx.GetT().Logf("Successfully stopped %s", containerName)
	return a
}

func (a *ClusterActions) RecoverFollower(nodeIndex int) *ClusterActions {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("Invalid nodeIndex %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}

	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	a.ctx.GetT().Log("Recovering follower")

	// #nosec G204 -- nodeIndex is range-checked above and forms a fixed broker-N container name.
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

func (a *ClusterActions) StopBroker(nodeIndex int) {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("invalid broker index %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}
	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	// #nosec G204 -- nodeIndex is range-checked above and forms a fixed broker-N container name.
	if err := exec.Command("docker", "stop", containerName).Run(); err != nil {
		a.ctx.GetT().Fatalf("failed to stop %s: %v", containerName, err)
	}
	a.ctx.GetClient().Close()
}

func (a *ClusterActions) StartBroker(nodeIndex int) {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("invalid broker index %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}
	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	// #nosec G204 -- nodeIndex is range-checked above and forms a fixed broker-N container name.
	if err := exec.Command("docker", "start", containerName).Run(); err != nil {
		a.ctx.GetT().Fatalf("failed to start %s: %v", containerName, err)
	}
	if err := a.waitForNodeHealth(nodeIndex, clusterHealthCheckAddrs(a.ctx.clusterSize)[nodeIndex-1]); err != nil {
		a.ctx.GetT().Fatalf("broker %d did not recover: %v", nodeIndex, err)
	}
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
	addrs := a.ctx.GetBrokerAddrs()
	if err := eventually(a.ctx.GetT(), fmt.Sprintf("topic metadata for %s", topic), clusterReadyTimeout, func() (bool, string, error) {
		lastDetail := "no broker returned metadata"
		for _, addr := range addrs {
			tempClient := e2e.NewBrokerClient([]string{addr})
			resp, err := tempClient.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
			tempClient.Close()
			if err != nil {
				lastDetail = fmt.Sprintf("%s: %v", addr, err)
				continue
			}
			if !strings.Contains(resp, "not found") && !strings.Contains(resp, "ERROR:") && strings.Contains(resp, topic) && strings.Contains(resp, "{") {
				return true, fmt.Sprintf("available on %s", addr), nil
			}
			lastDetail = fmt.Sprintf("%s: %s", addr, resp)
		}
		return false, lastDetail, nil
	}); err != nil {
		a.ctx.GetT().Fatal(err)
	}
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

	oldLeader, err := leaderAddressFromDescribe(resp)
	if err != nil {
		a.ctx.GetT().Fatalf("Could not identify leader for topic %s: %v; response: %s", topic, err, resp)
	}
	leaderNode, err := leaderNodeFromDescribe(resp, a.ctx.clusterSize)
	if err != nil {
		a.ctx.GetT().Fatalf("Could not identify leader for topic %s: %v; response: %s", topic, err, resp)
	}

	containerName := fmt.Sprintf("broker-%d", leaderNode)
	a.ctx.GetT().Logf("Stopping leader container: %s", containerName)

	// #nosec G204 -- leaderNode is parsed and range-checked against the configured cluster size.
	stopCmd := exec.Command("docker", "stop", containerName)
	if err := stopCmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to stop leader %s: %v", containerName, err)
	}

	a.ctx.GetClient().Close()

	if err := a.waitForPartitionLeaderChange(topic, oldLeader, 45*time.Second); err != nil {
		a.ctx.GetT().Fatal(err)
	}

	return leaderNode, a
}

func leaderNodeFromDescribe(resp string, clusterSize int) (int, error) {
	var metadata struct {
		Partitions []struct {
			Leader string `json:"leader"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal([]byte(resp), &metadata); err != nil {
		return 0, fmt.Errorf("decode topic metadata: %w", err)
	}
	if len(metadata.Partitions) == 0 || metadata.Partitions[0].Leader == "" {
		return 0, fmt.Errorf("topic metadata has no partition leader")
	}

	leaderHost := strings.SplitN(metadata.Partitions[0].Leader, ":", 2)[0]
	var leaderNode int
	if _, err := fmt.Sscanf(leaderHost, "broker-%d", &leaderNode); err != nil {
		return 0, fmt.Errorf("invalid leader address %q", metadata.Partitions[0].Leader)
	}
	if leaderNode < 1 || leaderNode > clusterSize || leaderHost != fmt.Sprintf("broker-%d", leaderNode) {
		return 0, fmt.Errorf("leader %q is outside the %d-node test cluster", leaderHost, clusterSize)
	}
	return leaderNode, nil
}

func leaderAddressFromDescribe(resp string) (string, error) {
	var metadata struct {
		Partitions []struct {
			Leader string `json:"leader"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal([]byte(resp), &metadata); err != nil {
		return "", fmt.Errorf("decode topic metadata: %w", err)
	}
	if len(metadata.Partitions) == 0 || metadata.Partitions[0].Leader == "" {
		return "", fmt.Errorf("topic metadata has no partition leader")
	}
	return metadata.Partitions[0].Leader, nil
}

func (a *ClusterActions) waitForPartitionLeaderChange(topic, oldLeader string, timeout time.Duration) error {
	return eventually(a.ctx.GetT(), fmt.Sprintf("partition leader change for %s", topic), timeout, func() (bool, string, error) {
		resp, err := a.actions.SendCommand(fmt.Sprintf("DESCRIBE topic=%s", topic))
		if err != nil {
			return false, "DESCRIBE failed", err
		}
		leader, err := leaderAddressFromDescribe(resp)
		if err != nil {
			return false, resp, err
		}
		return leader != oldLeader, fmt.Sprintf("leader=%s", leader), nil
	})
}
