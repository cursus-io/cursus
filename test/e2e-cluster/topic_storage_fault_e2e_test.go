package e2e_cluster

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/cursus-io/cursus/test/e2e"
)

const (
	runTopicStorageFaultE2E      = "RUN_E2E_TOPIC_STORAGE_FAULTS"
	storageFaultBaseExporterPort = 9100
	faultSentinelRoot            = "/run/cursus-e2e-faults"
	containerLogRoot             = "/root/cluster-logs"
	faultConvergenceTimeout      = 90 * time.Second
)

func TestTopicStorageFaultReconciliation(t *testing.T) {
	if os.Getenv(runTopicStorageFaultE2E) != "1" {
		t.Skipf("set %s=1 to run topic storage fault reconciliation", runTopicStorageFaultE2E)
	}

	ctx := GivenFaultClusterRestart(t)
	leader := waitForStorageFaultRaftLeader(t, ctx.clusterSize)
	target := firstFollower(leader, ctx.clusterSize)
	topicName := "fault-materialization"
	topicPath := filepath.ToSlash(filepath.Join(containerLogRoot, topicName))
	baselineTopics := waitForMetric(t, target, "cursus_broker_topics", nil, func(value float64) bool { return value >= 0 })

	setTopicStorageFault(t, target, "handler-open", topicName, true)
	t.Cleanup(func() { setTopicStorageFaultBestEffort(target, "handler-open", topicName, false) })
	requireDirectCommandSuccess(t, leader, fmt.Sprintf("CREATE topic=%s partitions=1", topicName))

	waitForFaultState(t, target, "create", http.StatusServiceUnavailable, 1)
	waitForContainerPath(t, target, topicPath, false)

	setTopicStorageFault(t, target, "handler-open", topicName, false)
	waitForFaultState(t, target, "create", http.StatusOK, 0)
	waitForMetric(t, target, "cursus_broker_topics", nil, func(value float64) bool { return value == baselineTopics+1 })
	for node := 1; node <= ctx.clusterSize; node++ {
		waitForContainerPath(t, node, topicPath, true)
	}

	setTopicStorageFault(t, target, "cleanup", topicName, true)
	t.Cleanup(func() { setTopicStorageFaultBestEffort(target, "cleanup", topicName, false) })
	requireDirectCommandSuccess(t, leader, fmt.Sprintf("DELETE topic=%s", topicName))

	waitForFaultState(t, target, "delete", http.StatusServiceUnavailable, 1)
	waitForMetric(t, target, "cursus_broker_topics", nil, func(value float64) bool { return value == baselineTopics })
	waitForContainerPath(t, target, topicPath, true)
	for node := 1; node <= ctx.clusterSize; node++ {
		if node != target {
			waitForContainerPath(t, node, topicPath, false)
		}
	}

	setTopicStorageFault(t, target, "cleanup", topicName, false)
	waitForFaultState(t, target, "delete", http.StatusOK, 0)
	waitForContainerPath(t, target, topicPath, false)
}

func waitForStorageFaultRaftLeader(t *testing.T, clusterSize int) int {
	t.Helper()
	leader := 0
	if err := eventually(t, "exactly one Raft leader metric", faultConvergenceTimeout, func() (bool, string, error) {
		leaders := make([]int, 0, 1)
		for node := 1; node <= clusterSize; node++ {
			value, err := readMetric(node, "cursus_cluster_is_leader", nil)
			if err != nil {
				return false, fmt.Sprintf("broker-%d metric unavailable", node), err
			}
			if value == 1 {
				leaders = append(leaders, node)
			}
		}
		if len(leaders) != 1 {
			return false, fmt.Sprintf("leaders=%v", leaders), nil
		}
		leader = leaders[0]
		return true, fmt.Sprintf("leader=broker-%d", leader), nil
	}); err != nil {
		t.Fatal(err)
	}
	return leader
}

func firstFollower(leader, clusterSize int) int {
	for node := 1; node <= clusterSize; node++ {
		if node != leader {
			return node
		}
	}
	return 0
}

func requireDirectCommandSuccess(t *testing.T, node int, command string) {
	t.Helper()
	client := e2e.NewBrokerClient([]string{fmt.Sprintf("127.0.0.1:%d", brokerPort(node))})
	defer client.Close()
	response, err := client.SendCommand("", command, 5*time.Second)
	if err != nil {
		t.Fatalf("broker-%d command %q failed: %v", node, command, err)
	}
	if strings.HasPrefix(strings.TrimSpace(response), "ERROR:") {
		t.Fatalf("broker-%d command %q returned %q", node, command, response)
	}
}

func waitForFaultState(t *testing.T, node int, operation string, readinessStatus int, pending float64) {
	t.Helper()
	description := fmt.Sprintf("broker-%d %s materialization state", node, operation)
	if err := eventually(t, description, faultConvergenceTimeout, func() (bool, string, error) {
		status, body, err := readReadiness(node)
		if err != nil {
			return false, "readiness unavailable", err
		}
		value, err := readMetric(node, "cursus_cluster_topic_materializations_pending", map[string]string{"operation": operation})
		if err != nil {
			return false, fmt.Sprintf("readiness=%d", status), err
		}
		bodyMatches := readinessStatus == http.StatusOK || strings.Contains(body, "topic_materialization")
		ready := status == readinessStatus && value == pending && bodyMatches
		return ready, fmt.Sprintf("readiness=%d pending=%v body=%s", status, value, body), nil
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForMetric(t *testing.T, node int, name string, labels map[string]string, condition func(float64) bool) float64 {
	t.Helper()
	var observed float64
	if err := eventually(t, fmt.Sprintf("broker-%d metric %s", node, name), faultConvergenceTimeout, func() (bool, string, error) {
		value, err := readMetric(node, name, labels)
		if err != nil {
			return false, "metric unavailable", err
		}
		observed = value
		return condition(value), fmt.Sprintf("value=%v labels=%v", value, labels), nil
	}); err != nil {
		t.Fatal(err)
	}
	return observed
}

func readMetric(node int, name string, labels map[string]string) (float64, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", storageFaultBaseExporterPort+node)) // #nosec G107 -- fixed loopback test endpoint.
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics status %d", response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return 0, err
	}
	family := families[name]
	if family == nil {
		return 0, fmt.Errorf("metric %s not found", name)
	}
	for _, metric := range family.Metric {
		if metricLabelsMatch(metric, labels) {
			switch family.GetType() {
			case dto.MetricType_COUNTER:
				return metric.GetCounter().GetValue(), nil
			case dto.MetricType_GAUGE:
				return metric.GetGauge().GetValue(), nil
			default:
				return 0, fmt.Errorf("metric %s has unsupported type %s", name, family.GetType())
			}
		}
	}
	return 0, fmt.Errorf("metric %s labels %v not found", name, labels)
}

func metricLabelsMatch(metric *dto.Metric, expected map[string]string) bool {
	if len(metric.Label) != len(expected) {
		return false
	}
	for _, label := range metric.Label {
		if expected[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

func readReadiness(node int) (int, string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", healthPort(node))) // #nosec G107 -- fixed loopback test endpoint.
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, "", err
	}
	return response.StatusCode, string(body), nil
}

func setTopicStorageFault(t *testing.T, node int, operation, topicName string, enabled bool) {
	t.Helper()
	if err := runTopicStorageFaultCommand(node, operation, topicName, enabled); err != nil {
		t.Fatalf("toggle broker-%d %s fault for %s: %v", node, operation, topicName, err)
	}
}

func setTopicStorageFaultBestEffort(node int, operation, topicName string, enabled bool) {
	_ = runTopicStorageFaultCommand(node, operation, topicName, enabled)
}

func runTopicStorageFaultCommand(node int, operation, topicName string, enabled bool) error {
	if node < 1 || node > 3 {
		return fmt.Errorf("invalid broker node %d", node)
	}
	if operation != "handler-open" && operation != "cleanup" {
		return fmt.Errorf("invalid fault operation %q", operation)
	}
	if strings.ContainsAny(topicName, "/\\") || topicName == "" {
		return fmt.Errorf("invalid fault topic %q", topicName)
	}
	service := fmt.Sprintf("broker-%d", node)
	directory := filepath.ToSlash(filepath.Join(faultSentinelRoot, operation))
	sentinel := filepath.ToSlash(filepath.Join(directory, topicName))
	composeFiles := []string{composeFile, faultComposeFile}
	if enabled {
		if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "mkdir", "-p", directory).CombinedOutput(); err != nil {
			return fmt.Errorf("create sentinel directory: %w: %s", err, output)
		}
		if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "touch", sentinel).CombinedOutput(); err != nil {
			return fmt.Errorf("create sentinel: %w: %s", err, output)
		}
		return nil
	}
	if output, err := runClusterCompose(composeFiles, "exec", "-T", service, "rm", "-f", sentinel).CombinedOutput(); err != nil {
		return fmt.Errorf("remove sentinel: %w: %s", err, output)
	}
	return nil
}

func waitForContainerPath(t *testing.T, node int, path string, exists bool) {
	t.Helper()
	description := fmt.Sprintf("broker-%d path %s exists=%t", node, path, exists)
	if err := eventually(t, description, faultConvergenceTimeout, func() (bool, string, error) {
		present, err := containerPathExists(node, path)
		if err != nil {
			return false, "container path check failed", err
		}
		return present == exists, fmt.Sprintf("present=%t", present), nil
	}); err != nil {
		t.Fatal(err)
	}
}

func containerPathExists(node int, path string) (bool, error) {
	service := fmt.Sprintf("broker-%d", node)
	cmd := runClusterCompose([]string{composeFile, faultComposeFile}, "exec", "-T", service, "test", "-d", path)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("inspect %s: %w: %s", path, err, output)
}
