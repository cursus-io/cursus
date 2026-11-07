package e2e_cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// captureCooperativeRecoveryFailure retains complete broker logs and bounded
// readiness/work snapshots before the fixture removes the failed cluster.
func captureCooperativeRecoveryFailure(t *testing.T) {
	t.Helper()
	if !t.Failed() {
		return
	}
	directory := os.Getenv("CURSUS_COOPERATIVE_RESULTS")
	if directory == "" {
		directory = t.TempDir()
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		t.Logf("cooperative evidence directory: %v", err)
		return
	}
	stamp := time.Now().UnixNano()
	for node := 1; node <= 3; node++ {
		prefix := filepath.Join(directory, fmt.Sprintf("failure-%d-broker-%d", stamp, node))
		for _, probe := range []struct{ name, url string }{
			{"ready", fmt.Sprintf("http://127.0.0.1:%d/ready", healthPort(node))},
			{"metrics", fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+clusterPortShift()+node)},
		} {
			captureCooperativeHTTP(t, prefix+"-"+probe.name+".txt", probe.url)
		}
		for _, command := range []struct {
			suffix string
			args   []string
		}{
			{".log", []string{"logs", cleanupContainerName(node)}},
			{"-state.json", []string{"inspect", "--format", "{{json .State}}", cleanupContainerName(node)}},
		} {
			file, err := os.Create(prefix + command.suffix)
			if err != nil {
				t.Logf("cooperative evidence file: %v", err)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cmd := exec.CommandContext(ctx, "docker", command.args...)
			cmd.Stdout, cmd.Stderr = file, file
			if err := cmd.Run(); err != nil {
				t.Logf("cooperative evidence %v: %v", command.args, err)
			}
			cancel()
			if err := file.Close(); err != nil {
				t.Logf("cooperative evidence close: %v", err)
			}
		}
	}
}

func captureCooperativeHTTP(t *testing.T, path, url string) {
	t.Helper()
	client := http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(url)
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(response.Body, 4<<20))
		_ = response.Body.Close()
		data = append([]byte(fmt.Sprintf("status=%d\n", response.StatusCode)), data...)
	}
	if err != nil {
		data = append(data, []byte(fmt.Sprintf("\nprobe error: %v\n", err))...)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Logf("cooperative evidence write: %v", err)
	}
}
