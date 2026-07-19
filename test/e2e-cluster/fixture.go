package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/test/e2e"
)

const (
	composeFile      = "docker-compose.yml"
	faultComposeFile = "docker-compose.faults.yml"
	baseBrokerPort   = 9000
	baseHealthPort   = 9080
)

// ClusterTestContext extends e2e.TestContext for cluster testing
type ClusterTestContext struct {
	*e2e.TestContext
	clusterSize       int
	minInSyncReplicas int
}

func brokerPort(nodeIndex int) int {
	return baseBrokerPort + nodeIndex // nodeIndex: 1-based
}

func healthPort(nodeIndex int) int {
	return baseHealthPort + nodeIndex // nodeIndex: 1-based
}

func clusterBrokerAddrs(size int) []string {
	addrs := make([]string, 0, size)
	for i := 1; i <= size; i++ {
		addrs = append(addrs,
			fmt.Sprintf("localhost:%d", brokerPort(i)),
		)
	}
	return addrs
}

func clusterHealthCheckAddrs(size int) []string {
	addrs := make([]string, 0, size)
	for i := 1; i <= size; i++ {
		addrs = append(addrs,
			fmt.Sprintf("http://localhost:%d/health", healthPort(i)),
		)
	}
	return addrs
}

// GivenCluster creates a new cluster test context
func GivenCluster(t *testing.T) *ClusterTestContext {
	ctx := e2e.Given(t)

	clusterSize := 3
	ctx.SetBrokerAddrs(clusterBrokerAddrs(clusterSize))

	return &ClusterTestContext{
		TestContext:       ctx,
		clusterSize:       clusterSize,
		minInSyncReplicas: 2,
	}
}

func GivenClusterRestart(t *testing.T) *ClusterTestContext {
	return givenClusterRestart(t, composeFile)
}

func GivenFaultClusterRestart(t *testing.T) *ClusterTestContext {
	return givenClusterRestart(t, composeFile, faultComposeFile)
}

func givenClusterRestart(t *testing.T, composeFiles ...string) *ClusterTestContext {
	_ = runClusterCompose(composeFiles, "down", "-v", "--remove-orphans").Run()
	cmd := runClusterCompose(composeFiles, "up", "-d", "--force-recreate", "--build")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start docker compose: %v\nOutput: %s", err, string(output))
	}

	t.Cleanup(func() {
		cmd := runClusterCompose(composeFiles, "down", "-v", "--remove-orphans")
		if err := cmd.Run(); err != nil {
			t.Logf("Cleanup warning: failed to bring down docker compose: %v", err)
		}
	})

	ctx := GivenCluster(t)
	t.Logf("Cluster startup initiated. Size: %d, MinISR: %d", ctx.clusterSize, ctx.minInSyncReplicas)

	actions := ctx.WhenCluster()
	if err := actions.checkAllNodesHealth(); err != nil {
		t.Fatalf("Cluster failed to stabilize within timeout: %v", err)
	}

	t.Log("Waiting for Raft leader to be elected...")
	if err := eventually(t, "Raft leader election", clusterReadyTimeout, func() (bool, string, error) {
		resp, err := actions.actions.SendCommand("LIST_CLUSTER")
		if err != nil {
			return false, "LIST_CLUSTER failed", err
		}
		return listClusterReady(resp)
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("Raft leader elected, cluster ready.")

	return ctx
}

func listClusterReady(response string) (bool, string, error) {
	const prefix = "OK brokers="
	response = strings.TrimSpace(response)
	if !strings.HasPrefix(response, prefix) {
		return false, response, fmt.Errorf("unexpected LIST_CLUSTER response %q", response)
	}
	var brokers []struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(response, prefix)), &brokers); err != nil {
		return false, response, fmt.Errorf("decode LIST_CLUSTER brokers: %w", err)
	}
	for _, broker := range brokers {
		if broker.Status == "active" {
			return true, fmt.Sprintf("%d broker(s), active member present", len(brokers)), nil
		}
	}
	return false, fmt.Sprintf("%d broker(s), no active member", len(brokers)), nil
}

func (c *ClusterTestContext) WithTopic(topic string) *ClusterTestContext {
	c.TestContext.WithTopic(topic)
	return c
}

func (c *ClusterTestContext) WithPartitions(partitions int) *ClusterTestContext {
	c.TestContext.WithPartitions(partitions)
	return c
}

func (c *ClusterTestContext) WithNumMessages(num int) *ClusterTestContext {
	c.TestContext.WithNumMessages(num)
	return c
}

func (c *ClusterTestContext) ResetPublishedCount() *ClusterTestContext {
	c.TestContext.ResetPublishedCount()
	return c
}

func (c *ClusterTestContext) WithAcks(acks string) *ClusterTestContext {
	c.TestContext.WithAcks(acks)
	return c
}

func (c *ClusterTestContext) WithClusterSize(size int) *ClusterTestContext {
	c.clusterSize = size
	c.SetBrokerAddrs(clusterBrokerAddrs(size))
	c.GetT().Logf("Cluster size configured to: %d", size)
	return c
}

func (c *ClusterTestContext) WithMinInSyncReplicas(min int) *ClusterTestContext {
	c.minInSyncReplicas = min
	return c
}

func runClusterCompose(composeFiles []string, args ...string) *exec.Cmd {
	composeArgs := make([]string, 0, len(composeFiles)*2+len(args))
	for _, file := range composeFiles {
		composeArgs = append(composeArgs, "-f", file)
	}
	composeArgs = append(composeArgs, args...)
	return e2e.RunCompose(composeArgs...)
}
