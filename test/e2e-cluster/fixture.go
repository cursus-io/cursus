package e2e_cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

const (
	composeFile    = "docker-compose.yml"
	baseBrokerPort = 9000
	baseHealthPort = 9080
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
	_ = e2e.RunCompose("-f", composeFile, "down", "-v", "--remove-orphans").Run()
	cmd := e2e.RunCompose("-f", composeFile, "up", "-d", "--force-recreate")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start docker compose: %v\nOutput: %s", err, string(output))
	}

	t.Cleanup(func() {
		cmd := e2e.RunCompose("-f", composeFile, "down", "-v")
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
	leaderElected := false
	for i := 0; i < 20; i++ {
		resp, err := actions.actions.SendCommand("LIST")
		if err == nil && !strings.Contains(resp, "ERROR:") && !strings.Contains(resp, "unknown") {
			leaderElected = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !leaderElected {
		t.Fatal("Cluster started but no leader was elected within 20 seconds")
	}
	t.Log("Raft leader elected, cluster ready.")

	return ctx
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
