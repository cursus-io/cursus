package e2e_cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestAcknowledgedClusterBackupRestore rehearses one frozen full-cluster backup
// generation on fresh writable volumes, preserving the originals until cleanup.
func TestAcknowledgedClusterBackupRestore(t *testing.T) {
	runAcknowledgedClusterBackupRestore(t, "", "")
}

// TestAcknowledgedClusterCoordinatedUpgrade seeds a pinned older revision and
// restores its complete backup using a different, explicitly recorded candidate.
func TestAcknowledgedClusterCoordinatedUpgrade(t *testing.T) {
	if os.Getenv("RUN_E2E_ACK_UPGRADE") != "1" {
		t.Skip("set RUN_E2E_ACK_UPGRADE=1 with archived source and candidate image")
	}
	source := os.Getenv("CURSUS_BACKUP_UPGRADE_SOURCE")
	image := os.Getenv("CURSUS_BACKUP_UPGRADE_IMAGE")
	require.NotEmpty(t, source)
	require.NotEmpty(t, image)
	revision, err := os.ReadFile(filepath.Join(source, ".upgrade-source-revision"))
	require.NoError(t, err)
	require.Equal(t, "c7384a51299a2519900966d3ed786b9cbe0e3f4c", strings.TrimSpace(string(revision)))
	runAcknowledgedClusterBackupRestore(t, source, image)
}

// runAcknowledgedClusterBackupRestore shares the identity and immutable-backup
// oracle between same-binary restoration and the coordinated revision change.
func runAcknowledgedClusterBackupRestore(t *testing.T, source, candidate string) {
	t.Helper()
	if os.Getenv("RUN_E2E_ACK_FAULTS") != "1" {
		t.Skip("set RUN_E2E_ACK_FAULTS=1 for isolated backup recovery")
	}
	backup := t.TempDir()
	var volumes []string
	// Register before the fixture: its container cleanup must run first.
	t.Cleanup(func() {
		for _, volume := range volumes {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			output, err := exec.CommandContext(cleanup, "docker", "volume", "rm", volume).CombinedOutput()
			cancel()
			if err != nil {
				t.Logf("backup volume cleanup %s: %v: %s", volume, err, output)
			}
		}
	})
	journal := newAckFaultJournal(t)
	ctx := newAckFaultClusterSource(t, "ack-backup", false, source)
	state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	for sequence := uint64(1); sequence <= 32; sequence++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, state.Leader), ackFaultMessage("backup-producer", sequence))
	}
	client, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), "backup-workers")
	response, err := client.SendCommand("", fmt.Sprintf("COMMIT_OFFSET topic=%s partition=0 group=backup-workers offset=13 member=%s generation=%d", ctx.GetTopic(), member, generation), 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, "OK", response)
	client.Close()
	ctx.GetClient().Close()
	compose := ackFaultDocker(t, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project.config_files"}}`, ackFaultContainer(1))
	require.NotContains(t, compose, ",", "the isolated fixture uses one generated Compose file")
	configuration, err := os.ReadFile(compose)
	require.NoError(t, err)
	images := make(map[int]string)
	for node := 1; node <= 3; node++ {
		images[node] = ackFaultDocker(t, "inspect", "--format", "{{.Image}}", ackFaultContainer(node))
	}
	restoreImages := make(map[int]string)
	candidateID := ""
	if candidate != "" {
		candidateID = ackFaultDocker(t, "image", "inspect", "--format", "{{.Id}}", candidate)
		revision := os.Getenv("CURSUS_BACKUP_UPGRADE_REVISION")
		require.Len(t, revision, 40, "record the candidate checkout revision")
		require.Equal(t, revision, ackFaultDocker(t, "image", "inspect", "--format", `{{index .Config.Labels "org.opencontainers.image.revision"}}`, candidateID))
		journal.record("upgrade_revisions", map[string]any{"source_commit": "c7384a51299a2519900966d3ed786b9cbe0e3f4c", "candidate_commit": revision, "candidate_image": candidateID})
	}
	for node := 1; node <= 3; node++ {
		restoreImages[node] = images[node]
		if candidateID != "" {
			require.NotEqual(t, images[node], candidateID, "upgrade must replace the source image")
			restoreImages[node] = candidateID
		}
	}
	imageID := images[1] // Offline file helpers use one of the recorded images.
	journal.record("backup_runtime", map[string]any{"images": images, "compose_sha256": fmt.Sprintf("%x", sha256.Sum256(configuration))})
	for node := 1; node <= 3; node++ {
		ackFaultDocker(t, "stop", "--time", "5", ackFaultContainer(node))
	}
	// No member is copied until every writer is confirmed stopped.
	for node := 1; node <= 3; node++ {
		require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(node)))
	}
	before := make(map[int]map[string]string)
	generationID := fmt.Sprintf("%x", time.Now().UnixNano())
	var copiedBytes int64
	for node := 1; node <= 3; node++ {
		original := ackFaultDocker(t, "inspect", "--format", `{{range .Mounts}}{{if eq .Destination "/app/cluster-logs"}}{{.Name}}{{end}}{{end}}`, ackFaultContainer(node))
		require.True(t, strings.HasPrefix(original, ackFaultProject+"_broker-"), "only fixture-owned volumes may be copied or removed: %s", original)
		volumes = append(volumes, original)
		directory := filepath.Join(backup, fmt.Sprintf("broker-%d", node))
		require.NoError(t, os.Mkdir(directory, 0o700))
		ackFaultDocker(t, "cp", ackFaultContainer(node)+":/app/cluster-logs/.", directory)
		inventory, size := backupFileInventory(t, directory)
		before[node] = inventory
		copiedBytes += size
		remote := ackFaultDocker(t, "run", "--rm", "--network", "none", "--volumes-from", ackFaultContainer(node)+":ro", "--entrypoint", "sh", imageID, "-c", "cd /app/cluster-logs && find . -type f -exec sha256sum {} +")
		require.Equal(t, inventory, parseBackupHashes(t, remote), "source/copy mismatch for broker %d", node)
	}
	journal.record("backup_generation", map[string]any{"members": 3, "copied_bytes": copiedBytes, "source_hash_bytes": copiedBytes, "copy_hash_bytes": copiedBytes, "files": before})
	var document map[string]any
	require.NoError(t, yaml.Unmarshal(configuration, &document))
	declarations, ok := document["volumes"].(map[string]any)
	require.True(t, ok)
	services, ok := document["services"].(map[string]any)
	require.True(t, ok)
	for node := 1; node <= 3; node++ {
		clone := fmt.Sprintf("%s-restore-%s-broker-%d", ackFaultProject, generationID, node)
		volumes = append(volumes, clone)
		ackFaultDocker(t, "volume", "create", clone)
		directory := filepath.Join(backup, fmt.Sprintf("broker-%d", node))
		ackFaultDocker(t, "run", "--rm", "--network", "none", "--user", "0", "-v", directory+":/backup:ro", "-v", clone+":/restore", "--entrypoint", "sh", imageID, "-c", "cp -a /backup/. /restore/ && chown -R 1000:1000 /restore")
		remote := ackFaultDocker(t, "run", "--rm", "--network", "none", "-v", clone+":/restore:ro", "--entrypoint", "sh", imageID, "-c", "cd /restore && find . -type f -exec sha256sum {} +")
		require.Equal(t, before[node], parseBackupHashes(t, remote), "backup/clone mismatch for broker %d", node)
		declarations[fmt.Sprintf("broker-%d-data", node)] = map[string]any{"external": true, "name": clone}
		service, ok := services[fmt.Sprintf("broker-%d", node)].(map[string]any)
		require.True(t, ok)
		service["image"] = restoreImages[node]
		delete(service, "build")
	}
	restoreConfiguration, err := yaml.Marshal(document)
	require.NoError(t, err)
	// The fixture's existing cleanup reads this same file. External cloned
	// volumes are removed by the separate, pre-registered cleanup above.
	require.NoError(t, os.WriteFile(compose, restoreConfiguration, 0o600))
	for node := 1; node <= 3; node++ {
		ackFaultDocker(t, "rm", ackFaultContainer(node))
	}
	began := time.Now()
	ackFaultDocker(t, "compose", "-p", ackFaultProject, "-f", compose, "up", "-d", "--no-build", "--pull", "never", "--force-recreate")
	require.NoError(t, ctx.WhenCluster().checkAllNodesHealth())
	state = waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	for node := 1; node <= 3; node++ {
		require.Equal(t, restoreImages[node], ackFaultDocker(t, "inspect", "--format", "{{.Image}}", ackFaultContainer(node)))
	}
	response, err = ctx.GetClient().SendCommand("", "FETCH_OFFSET topic="+ctx.GetTopic()+" partition=0 group=backup-workers", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, "OK offset=13", response)
	publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, state.Leader), ackFaultMessage("backup-producer", 33))
	verifyAckFaultRecovery(t, ctx, journal)
	for node := 1; node <= 3; node++ {
		inventory, _ := backupFileInventory(t, filepath.Join(backup, fmt.Sprintf("broker-%d", node)))
		require.Equal(t, before[node], inventory, "restore must not modify the backup generation")
	}
	journal.record("backup_restore_verified", map[string]any{"elapsed_ms": time.Since(began).Milliseconds(), "cloned_bytes": copiedBytes, "clone_hash_bytes": copiedBytes, "backup_recheck_bytes": copiedBytes, "committed_offset": 13, "acknowledged_records": 33})
}

func backupFileInventory(t *testing.T, root string) (map[string]string, int64) {
	t.Helper()
	result := make(map[string]string)
	var total int64
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular backup entry %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		digest := sha256.New()
		count, err := io.Copy(digest, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[filepath.ToSlash(relative)] = hex.EncodeToString(digest.Sum(nil))
		total += count
		return nil
	}))
	return result, total
}

func parseBackupHashes(t *testing.T, output string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		digest, path, ok := strings.Cut(strings.TrimSpace(line), "  ./")
		require.True(t, ok, "invalid file digest row: %s", line)
		require.Len(t, digest, 64)
		require.NotContains(t, result, path, "duplicate backup path")
		result[path] = digest
	}
	return result
}
