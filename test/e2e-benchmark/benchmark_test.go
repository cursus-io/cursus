package e2e_benchmark

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const benchmarkTimeout = 5 * time.Minute

func getComposeCommand() []string {
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}
	}
	return []string{"docker", "compose"}
}

func composeFile(rel string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot resolve benchmark compose path")
	}
	return filepath.Join(filepath.Dir(file), rel)
}

func runCompose(args ...string) *exec.Cmd {
	base := getComposeCommand()
	fullArgs := append([]string{}, base[1:]...)
	fullArgs = append(fullArgs, "--project-name", "cursus-benchmark")
	fullArgs = append(fullArgs, args...)
	cmd := exec.Command(base[0], fullArgs...)
	return cmd
}

func TestRunComposePlacesProjectNameAfterComposeSubcommand(t *testing.T) {
	cmd := runCompose("-f", "docker-compose.yml", "up", "-d")
	args := cmd.Args[1:]

	if len(args) == 0 {
		t.Fatalf("expected compose arguments")
	}
	if args[0] == "compose" {
		if len(args) < 3 || args[1] != "--project-name" || args[2] != "cursus-benchmark" {
			t.Fatalf("docker compose project name must follow compose subcommand, got %v", args)
		}
		return
	}
	if args[0] != "--project-name" || len(args) < 2 || args[1] != "cursus-benchmark" {
		t.Fatalf("docker-compose project name must be first argument, got %v", args)
	}
}

func composeDown(t *testing.T, file string) {
	// Force remove containers first to handle conflicts from compose files that
	// declare fixed container_name values outside project-name isolation.
	for _, name := range fixedBenchmarkContainers(file) {
		cmd := exec.Command("docker", "rm", "-f", "-v", name)
		if output, err := cmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			t.Logf("docker rm warning for %s: %v\n%s", name, err, string(output))
		}
	}

	cmd := runCompose("-f", file, "rm", "-f", "-s", "-v")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose rm warning: %v\n%s", err, string(output))
	}

	cmd = runCompose("-f", file, "down", "-v", "--remove-orphans")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose down warning: %v\n%s", err, string(output))
	}
}

func fixedBenchmarkContainers(file string) []string {
	if strings.Contains(filepath.ToSlash(file), "/cluster/") {
		return []string{"broker-1", "broker-2", "broker-3", "broker-publisher", "broker-consumer", "prometheus"}
	}
	return []string{"bench-broker", "bench-publisher", "bench-consumer"}
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

	lower := strings.ToLower(logs)
	failureMarkers := []string{
		"fatal",
		"panic",
		"benchmark incomplete",
		"verify failed",
		"failed messages              : 1",
		"failed messages              : 2",
		"failed messages              : 3",
		"failed messages              : 4",
		"failed messages              : 5",
		"failed messages              : 6",
		"failed messages              : 7",
		"failed messages              : 8",
		"failed messages              : 9",
		"message missing       : 1",
		"message missing       : 2",
		"message missing       : 3",
		"message missing       : 4",
		"message missing       : 5",
		"message missing       : 6",
		"message missing       : 7",
		"message missing       : 8",
		"message missing       : 9",
	}
	for _, marker := range failureMarkers {
		if strings.Contains(lower, marker) {
			t.Fatalf("%s benchmark reported failure marker %q:\n%s", component, marker, lastLines(logs, 40))
		}
	}

	if component == "Publisher" {
		if !strings.Contains(logs, "Failed messages              : 0") || !strings.Contains(logs, "rate: 100.00%") {
			t.Fatalf("publisher did not report a complete publish:\n%s", lastLines(logs, 40))
		}
	} else if component == "Consumer" {
		if !strings.Contains(logs, "All messages consumed") || !strings.Contains(logs, "Message missing       : 0") {
			t.Fatalf("consumer did not report complete consumption:\n%s", lastLines(logs, 40))
		}
	}

	t.Logf("%s benchmark completed successfully", component)
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

	composeDown(t, composeFile("../docker-compose.yml"))
	t.Cleanup(func() { composeDown(t, composeFile("../docker-compose.yml")) })

	t.Log("Starting standalone benchmark...")
	composeUp(t, composeFile("../docker-compose.yml"))

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, pubDone := waitForContainerExit(t, composeFile("../docker-compose.yml"), "publisher", "bench-publisher", benchmarkTimeout)
	if !pubDone {
		t.Fatal("Publisher did not complete within timeout")
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, consDone := waitForContainerExit(t, composeFile("../docker-compose.yml"), "consumer", "bench-consumer", benchmarkTimeout)
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

	composeDown(t, composeFile("../cluster/docker-compose.yml"))
	t.Cleanup(func() { composeDown(t, composeFile("../cluster/docker-compose.yml")) })

	t.Log("Starting cluster benchmark (3 nodes)...")
	composeUp(t, composeFile("../cluster/docker-compose.yml"))

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, pubDone := waitForContainerExit(t, composeFile("../cluster/docker-compose.yml"), "publisher", "broker-publisher", benchmarkTimeout)
	if !pubDone {
		t.Fatal("Publisher did not complete within timeout")
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, consDone := waitForContainerExit(t, composeFile("../cluster/docker-compose.yml"), "consumer", "broker-consumer", benchmarkTimeout)
	if !consDone {
		t.Fatal("Consumer did not complete within timeout")
	}
	assertBenchmarkSuccess(t, consLogs, "Consumer")
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
