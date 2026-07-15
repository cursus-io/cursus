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
			payload := strings.TrimPrefix(strings.TrimSpace(resp), "OK brokers=")
			var brokers []fsm.BrokerInfo
			if err := json.Unmarshal([]byte(payload), &brokers); err == nil {
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

	a.ctx.GetT().Fatalf("cluster did not report %d active nodes after %d attempts", a.ctx.clusterSize, maxRetries)
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
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := a.actions.SendCommand(fmt.Sprintf("DESCRIBE topic=%s", topic))
		if err == nil {
			leader, parseErr := leaderAddressFromDescribe(resp)
			switch {
			case parseErr != nil:
				lastErr = parseErr
			case leader != oldLeader:
				a.ctx.GetT().Logf("Partition leader changed from %s to %s", oldLeader, leader)
				return nil
			default:
				lastErr = fmt.Errorf("partition leader is still %s", oldLeader)
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("partition leader did not change from %s within %s: %w", oldLeader, timeout, lastErr)
}
