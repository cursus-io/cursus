package e2e_cluster

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

const (
	runClusterFailureE2E = "RUN_E2E_CLUSTER_FAILURE"
	clusterFailureWait   = 90 * time.Second
	baseExporterPort     = 9100
)

func TestRaftLeaderFailoverPropagatesDirectTopicMutations(t *testing.T) {
	if os.Getenv(runClusterFailureE2E) != "1" {
		t.Skipf("set %s=1 to run the compose cluster failure test", runClusterFailureE2E)
	}

	startFailureTestCluster(t)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
	oldLeader := waitForSingleRaftLeader(t, []int{1, 2, 3}, 0)

	stopComposeBroker(t, oldLeader)
	survivors := survivorNodes(oldLeader)
	newLeader := waitForSingleRaftLeader(t, survivors, oldLeader)
	waitForAllBrokerReadiness(t, survivors)

	topicName := "raft-failover-direct-mutations"
	createResponse, err := sendDirectBrokerCommand(newLeader, fmt.Sprintf("CREATE topic=%s partitions=1", topicName))
	if err != nil || strings.Contains(strings.ToUpper(createResponse), "ERROR") {
		t.Fatalf("direct CREATE through elected survivor broker-%d failed: response=%q err=%v", newLeader, createResponse, err)
	}
	waitForTopicPresence(t, survivors, topicName, true)

	deleteResponse, err := sendDirectBrokerCommand(newLeader, fmt.Sprintf("DELETE topic=%s", topicName))
	if err != nil || strings.Contains(strings.ToUpper(deleteResponse), "ERROR") {
		t.Fatalf("direct DELETE through elected survivor broker-%d failed: response=%q err=%v", newLeader, deleteResponse, err)
	}
	waitForTopicPresence(t, survivors, topicName, false)
}

func startFailureTestCluster(t *testing.T) {
	t.Helper()
	_ = e2e.RunCompose("-f", composeFile, "down", "-v", "--remove-orphans").Run()
	cmd := e2e.RunCompose("-f", composeFile, "up", "-d", "--build", "--force-recreate")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start compose failure cluster: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		if output, err := e2e.RunCompose("-f", composeFile, "down", "-v", "--remove-orphans").CombinedOutput(); err != nil {
			t.Logf("compose cleanup failed: %v\n%s", err, output)
		}
	})
}

func stopComposeBroker(t *testing.T, node int) {
	t.Helper()
	service := fmt.Sprintf("broker-%d", node)
	if output, err := e2e.RunCompose("-f", composeFile, "stop", service).CombinedOutput(); err != nil {
		t.Fatalf("stop %s: %v\n%s", service, err, output)
	}
}

func survivorNodes(stopped int) []int {
	nodes := make([]int, 0, 2)
	for node := 1; node <= 3; node++ {
		if node != stopped {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func waitForAllBrokerReadiness(t *testing.T, nodes []int) {
	t.Helper()
	httpClient := &http.Client{Timeout: 2 * time.Second}
	pollClusterFailure(t, "broker readiness", func() (bool, string) {
		for _, node := range nodes {
			url := fmt.Sprintf("http://127.0.0.1:%d/ready", healthPort(node))
			response, err := httpClient.Get(url) // #nosec G107 -- loopback ports are fixed by the test compose file.
			if err != nil {
				return false, fmt.Sprintf("broker-%d: %v", node, err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return false, fmt.Sprintf("broker-%d readiness status=%d", node, response.StatusCode)
			}
		}
		return true, "all requested brokers ready"
	})
}

func waitForSingleRaftLeader(t *testing.T, nodes []int, excluded int) int {
	t.Helper()
	leader := 0
	pollClusterFailure(t, "exactly one Raft leader", func() (bool, string) {
		leaders := make([]int, 0, 1)
		for _, node := range nodes {
			value, err := fetchMetric(node, "cursus_cluster_is_leader")
			if err != nil {
				return false, fmt.Sprintf("broker-%d metrics: %v", node, err)
			}
			if value == 1 {
				leaders = append(leaders, node)
			}
		}
		if len(leaders) != 1 || leaders[0] == excluded {
			return false, fmt.Sprintf("leaders=%v excluded=%d", leaders, excluded)
		}
		leader = leaders[0]
		return true, fmt.Sprintf("leader=broker-%d", leader)
	})
	return leader
}

func fetchMetric(node int, metricName string) (float64, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+node)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url) // #nosec G107 -- loopback ports are fixed by the test compose file.
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == metricName {
			return strconv.ParseFloat(fields[1], 64)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("metric %s not found", metricName)
}

func sendDirectBrokerCommand(node int, command string) (string, error) {
	client := e2e.NewBrokerClient([]string{fmt.Sprintf("127.0.0.1:%d", brokerPort(node))})
	defer client.Close()
	return client.SendCommand("", command, 5*time.Second)
}

func waitForTopicPresence(t *testing.T, nodes []int, topicName string, present bool) {
	t.Helper()
	description := "topic deletion propagation"
	if present {
		description = "topic creation propagation"
	}
	pollClusterFailure(t, description, func() (bool, string) {
		for _, node := range nodes {
			response, err := sendDirectBrokerCommand(node, fmt.Sprintf("DESCRIBE topic=%s", topicName))
			if err != nil {
				return false, fmt.Sprintf("broker-%d: %v", node, err)
			}
			missing := strings.Contains(strings.ToLower(response), "not found") || strings.Contains(strings.ToUpper(response), "ERROR")
			if present && missing {
				return false, fmt.Sprintf("broker-%d has no topic: %s", node, response)
			}
			if !present && !missing {
				return false, fmt.Sprintf("broker-%d still has topic: %s", node, response)
			}
		}
		return true, fmt.Sprintf("topic present=%t on nodes=%v", present, nodes)
	})
}

func pollClusterFailure(t *testing.T, description string, condition func() (bool, string)) {
	t.Helper()
	err := eventually(t, description, clusterFailureWait, func() (bool, string, error) {
		ok, detail := condition()
		return ok, detail, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
