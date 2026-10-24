package e2e_cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cleanupProfileSample struct {
	At         time.Time          `json:"at"`
	Completed  time.Time          `json:"completed"`
	ProcIO     string             `json:"proc_io"`
	ProcStatus string             `json:"proc_status"`
	BlockIO    string             `json:"cgroup_io"`
	Network    string             `json:"network"`
	CPU        string             `json:"cgroup_cpu"`
	Sources    map[string]string  `json:"cgroup_sources"`
	Metrics    map[string]float64 `json:"metrics"`
}

// Read raw counters as well as their timestamps. A missing series stays absent;
// callers must not infer zero physical I/O from zero cleanup replay records.
func sampleCleanupProfile(t *testing.T, node int) cleanupProfileSample {
	t.Helper()
	sample := cleanupProfileSample{At: time.Now().UTC(), Metrics: make(map[string]float64), Sources: make(map[string]string)}
	for _, entry := range []struct {
		path   string
		output *string
	}{
		{"/proc/1/io", &sample.ProcIO}, {"/proc/1/status", &sample.ProcStatus},
		{"/proc/net/dev", &sample.Network},
	} {
		out, err := exec.Command("docker", "exec", "-u", "0", cleanupContainerName(node), "cat", entry.path).CombinedOutput()
		require.NoError(t, err, string(out))
		*entry.output = string(out)
	}
	read := func(path string) (string, error) {
		out, err := exec.Command("docker", "exec", "-u", "0", cleanupContainerName(node), "cat", path).CombinedOutput()
		return string(out), err
	}
	// Docker Desktop may expose cgroup v1 without a blkio accounting controller.
	// Required broker /proc I/O remains measured; absent cgroup data is not zero.
	sample.Sources["io"] = "unavailable"
	for _, path := range []string{"/sys/fs/cgroup/io.stat", "/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes"} {
		if value, err := read(path); err == nil {
			sample.BlockIO, sample.Sources["io"] = value, path
			break
		}
	}
	if value, err := read("/sys/fs/cgroup/cpu.stat"); err == nil {
		sample.CPU, sample.Sources["cpu"] = value, "/sys/fs/cgroup/cpu.stat"
	} else {
		value, err := read("/sys/fs/cgroup/cpuacct/cpuacct.usage")
		require.NoError(t, err, value)
		sample.CPU = "usage_nsec " + strings.TrimSpace(value) + "\n"
		sample.Sources["cpu"] = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
	}
	values, err := readCleanupProfileMetrics(node)
	require.NoError(t, err)
	sample.Metrics = values
	sample.Completed = time.Now().UTC()
	return sample
}

func readCleanupProfileMetrics(node int) (map[string]float64, error) {
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+clusterPortShift()+node))
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics HTTP %d", response.StatusCode)
	}
	values := make(map[string]float64)
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "consumer_cleanup_") || strings.HasPrefix(line, "cursus_recovery_") || strings.HasPrefix(line, "cursus_consumer_history_") || strings.HasPrefix(line, "process_") || strings.HasPrefix(line, "go_memstats_heap_") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				if value, err := strconv.ParseFloat(fields[1], 64); err == nil {
					values[fields[0]] = value
				}
			}
		}
	}
	return values, scanner.Err()
}

// Attach before the measured reset and stop only the tracer afterwards. No
// ptrace capability or tracing command is enabled in production containers.
func startCleanupSyncTrace(t *testing.T, node int) func() string {
	t.Helper()
	const command = "echo $$ > /run/cursus-e2e-faults/profile-tracer.pid; exec strace -f -c -e trace=fsync,fdatasync -o /run/cursus-e2e-faults/profile-sync.txt -p 1"
	out, err := exec.Command("docker", "exec", "-d", "-u", "0", cleanupContainerName(node), "sh", "-c", command).CombinedOutput()
	require.NoError(t, err, string(out))
	require.NoError(t, eventually(t, "sync tracer attached", 10*time.Second, func() (bool, string, error) {
		out, err := exec.Command("docker", "exec", "-u", "0", cleanupContainerName(node), "cat", "/proc/1/status").CombinedOutput()
		if err != nil {
			return false, string(out), err
		}
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "TracerPid:") {
				return strings.TrimSpace(strings.TrimPrefix(line, "TracerPid:")) != "0", line, nil
			}
		}
		return false, "no tracer identity", nil
	}))
	return func() string {
		out, err := exec.Command("docker", "exec", "-u", "0", cleanupContainerName(node), "sh", "-c", "kill -INT $(cat /run/cursus-e2e-faults/profile-tracer.pid)").CombinedOutput()
		require.NoError(t, err, string(out))
		var summary string
		require.NoError(t, eventually(t, "sync trace flushed", 10*time.Second, func() (bool, string, error) {
			out, err := exec.Command("docker", "exec", "-u", "0", cleanupContainerName(node), "cat", "/run/cursus-e2e-faults/profile-sync.txt").CombinedOutput()
			summary = string(out)
			return err == nil && strings.Contains(summary, "total"), summary, nil
		}))
		return summary
	}
}

func writeCleanupProfile(t *testing.T, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	t.Logf("cleanup_profile=%s", data)
	if path := os.Getenv("CURSUS_CLEANUP_PROFILE_OUTPUT"); path != "" {
		require.NoError(t, os.WriteFile(path, data, 0600))
	}
}
