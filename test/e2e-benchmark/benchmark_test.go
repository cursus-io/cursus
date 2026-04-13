package e2e_benchmark

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	standaloneCompose = "../docker-compose.yml"
	clusterCompose    = "../cluster/docker-compose.yml"

	benchmarkTimeout = 5 * time.Minute
)

func getComposeCommand() []string {
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}
	}
	return []string{"docker", "compose"}
}

func runCompose(args ...string) *exec.Cmd {
	base := getComposeCommand()
	fullArgs := append(base[1:], args...)
	cmd := exec.Command(base[0], fullArgs...)
	return cmd
}

func composeDown(t *testing.T, file string) {
	// Force remove containers first to handle conflicts from other compose projects
	// sharing the same container names
	cmd := runCompose("-f", file, "rm", "-f", "-s", "-v")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose rm warning: %v\n%s", err, string(output))
	}

	cmd = runCompose("-f", file, "down", "-v", "--remove-orphans")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose down warning: %v\n%s", err, string(output))
	}
}

func composeUp(t *testing.T, file string) {
	cmd := runCompose("-f", file, "up", "-d", "--build", "--force-recreate")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compose up failed: %v\n%s", err, string(output))
	}
}

func waitForContainerExit(t *testing.T, file, service, container string, timeout time.Duration) (string, bool) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			// Dump logs for debugging (compose logs uses service name)
			logsCmd := runCompose("-f", file, "logs", "--tail", "50", service)
			logs, _ := logsCmd.CombinedOutput()
			t.Logf("Timeout waiting for %s. Last logs:\n%s", container, string(logs))
			return string(logs), false
		case <-ticker.C:
			// Check if container has exited (docker inspect uses container name)
			inspectCmd := exec.Command("docker", "inspect", "-f", "{{.State.Status}}", container)
			out, err := inspectCmd.Output()
			if err != nil {
				continue
			}
			status := strings.TrimSpace(string(out))
			if status == "exited" {
				logsCmd := runCompose("-f", file, "logs", service)
				logs, _ := logsCmd.CombinedOutput()
				return string(logs), true
			}
		}
	}
}

func assertBenchmarkSuccess(t *testing.T, logs string, component string) {
	t.Helper()

	if strings.Contains(logs, "All messages consumed") ||
		strings.Contains(logs, "Benchmark completed") ||
		strings.Contains(logs, "Consumer closed gracefully") ||
		strings.Contains(logs, "All messages published") ||
		strings.Contains(logs, "Publisher closed gracefully") {
		t.Logf("%s benchmark completed successfully", component)
		return
	}

	if strings.Contains(logs, "FATAL") || strings.Contains(logs, "panic") {
		t.Errorf("%s crashed during benchmark:\n%s", component, lastLines(logs, 30))
		return
	}

	// Check exit code
	if strings.Contains(logs, "exit code 0") {
		t.Logf("%s exited (likely success)", component)
		return
	}

	t.Errorf("%s benchmark did not complete successfully:\n%s", component, lastLines(logs, 30))
}

func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

// TestStandaloneBenchmark runs the standalone benchmark and verifies produce/consume complete.
func TestStandaloneBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	composeDown(t, standaloneCompose)
	t.Cleanup(func() { composeDown(t, standaloneCompose) })

	t.Log("Starting standalone benchmark...")
	composeUp(t, standaloneCompose)

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, pubDone := waitForContainerExit(t, standaloneCompose, "publisher", "bench-publisher", benchmarkTimeout)
	if !pubDone {
		t.Fatal("Publisher did not complete within timeout")
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, consDone := waitForContainerExit(t, standaloneCompose, "consumer", "bench-consumer", benchmarkTimeout)
	if !consDone {
		t.Fatal("Consumer did not complete within timeout")
	}
	assertBenchmarkSuccess(t, consLogs, "Consumer")
}

// TestClusterBenchmark runs the 3-node cluster benchmark and verifies produce/consume complete.
func TestClusterBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	composeDown(t, clusterCompose)
	t.Cleanup(func() { composeDown(t, clusterCompose) })

	t.Log("Starting cluster benchmark (3 nodes)...")
	composeUp(t, clusterCompose)

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, pubDone := waitForContainerExit(t, clusterCompose, "publisher", "broker-publisher", benchmarkTimeout)
	if !pubDone {
		t.Fatal("Publisher did not complete within timeout")
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, consDone := waitForContainerExit(t, clusterCompose, "consumer", "broker-consumer", benchmarkTimeout)
	if !consDone {
		t.Fatal("Consumer did not complete within timeout")
	}
	assertBenchmarkSuccess(t, consLogs, "Consumer")
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
