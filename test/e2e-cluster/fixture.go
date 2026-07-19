package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/test/e2e"
)

const (
	composeFile         = "docker-compose.yml"
	snapshotComposeFile = "docker-compose.snapshot.yml"
	snapshotProjectName = "cursus-snapshot-e2e"
	baseBrokerPort      = 9000
	baseHealthPort      = 9080
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
	return givenClusterRestart(t, "", composeFile)
}

func GivenSnapshotClusterRestart(t *testing.T) *ClusterTestContext {
	return givenClusterRestart(t, snapshotProjectName, composeFile, snapshotComposeFile)
}

func givenClusterRestart(t *testing.T, project string, composeFiles ...string) *ClusterTestContext {
	downArgs := composeCommandArgs(project, composeFiles, "down", "-v", "--remove-orphans")
	_ = e2e.RunCompose(downArgs...).Run()
	upArgs := composeCommandArgs(project, composeFiles, "up", "-d", "--force-recreate", "--build")
	cmd := e2e.RunCompose(upArgs...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start docker compose: %v\nOutput: %s", err, string(output))
	}

	t.Cleanup(func() {
		cmd := e2e.RunCompose(composeCommandArgs(project, composeFiles, "down", "-v", "--remove-orphans")...)
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
		return clusterReadinessFromListCluster(resp)
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("Raft leader elected, cluster ready.")

	return ctx
}

func clusterReadinessFromListCluster(resp string) (bool, string, error) {
	const prefix = "OK brokers="
	trimmed := strings.TrimSpace(resp)
	if !strings.HasPrefix(trimmed, prefix) {
		return false, trimmed, nil
	}

	var brokers []fsm.BrokerInfo
	if err := json.Unmarshal([]byte(strings.TrimPrefix(trimmed, prefix)), &brokers); err != nil {
		return false, trimmed, fmt.Errorf("decode LIST_CLUSTER: %w", err)
	}
	active := 0
	for _, broker := range brokers {
		if strings.EqualFold(broker.Status, "active") {
			active++
		}
	}
	detail := fmt.Sprintf("%d/%d active", active, len(brokers))
	return active > 0, detail, nil
}

func composeCommandArgs(project string, composeFiles []string, command ...string) []string {
	args := make([]string, 0, len(composeFiles)*2+len(command)+2)
	if project != "" {
		args = append(args, "-p", project)
	}
	for _, file := range composeFiles {
		args = append(args, "-f", file)
	}
	return append(args, command...)
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
