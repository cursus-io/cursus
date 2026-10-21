package e2e_cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func cleanupPortOffset() int {
	value := os.Getenv("CURSUS_CLEANUP_PORT_OFFSET")
	if value == "" {
		return 0
	}
	offset, err := strconv.Atoi(value)
	if err != nil || offset < 10000 || offset > 40000 {
		panic("CURSUS_CLEANUP_PORT_OFFSET must be between 10000 and 40000")
	}
	return offset
}

func cleanupContainerName(node int) string {
	if offset := cleanupPortOffset(); offset != 0 {
		return fmt.Sprintf("cursus-truncate-%d-broker-%d", offset, node)
	}
	return fmt.Sprintf("cursus-cleanup-broker-%d", node)
}

// Isolated host ports, project, container names and volumes allow this destructive
// test fixture to run without stopping an existing developer cluster.
func isolateCleanupCompose(t *testing.T, project string, files []string) (string, []string) {
	t.Helper()
	offset := cleanupPortOffset()
	if offset == 0 {
		return project, files
	}
	require.Equal(t, "cursus-cleanup-e2e", project, "port override is supported only by cleanup scenarios")
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	configPath, err := filepath.Abs("../cluster/config.yaml")
	require.NoError(t, err)
	data, err := os.ReadFile(composeFile)
	require.NoError(t, err)
	text := strings.ReplaceAll(string(data), "context: ../..", "context: "+filepath.ToSlash(root))
	text = strings.ReplaceAll(text, "../cluster/config.yaml", filepath.ToSlash(configPath))
	for node := 1; node <= 3; node++ {
		text = strings.ReplaceAll(text, fmt.Sprintf("container_name: broker-%d", node), "container_name: "+cleanupContainerName(node))
		text = strings.ReplaceAll(text, fmt.Sprintf("ADVERTISED_BROKER_PORT=%d", 9000+node), fmt.Sprintf("ADVERTISED_BROKER_PORT=%d", 9000+node+offset))
		for _, port := range []int{9000 + node, 7999 + node, 9080 + node, 9100 + node} {
			text = strings.ReplaceAll(text, fmt.Sprintf(`"%d:`, port), fmt.Sprintf(`"%d:`, port+offset))
		}
		text = strings.ReplaceAll(text, fmt.Sprintf("127.0.0.1:%d:", 19000+node), fmt.Sprintf("127.0.0.1:%d:", 19000+node+offset))
	}
	project = fmt.Sprintf("cursus-truncate-%d-e2e", offset)
	text = strings.ReplaceAll(text, "name: cluster_network", "name: "+project)
	outputDir := t.TempDir()
	outputRoot, err := os.OpenRoot(outputDir)
	require.NoError(t, err)
	defer func() { _ = outputRoot.Close() }()
	base := filepath.Join(outputDir, "compose.yml")
	require.NoError(t, outputRoot.WriteFile("compose.yml", []byte(text), 0o600))
	isolated := []string{base}
	for _, file := range files {
		if file != composeFile && file != "docker-compose.consumer-cleanup.yml" {
			absolute, err := filepath.Abs(file)
			require.NoError(t, err)
			if file == "docker-compose.consumer-cleanup-recovery.yml" {
				override, readErr := os.ReadFile("docker-compose.consumer-cleanup-recovery.yml")
				require.NoError(t, readErr)
				absolute = filepath.Join(filepath.Dir(base), "recovery.yml")
				require.NoError(t, outputRoot.WriteFile("recovery.yml", []byte(strings.ReplaceAll(string(override), "cursus-cleanup-recovery:local", project+":local")), 0o600))
			}
			isolated = append(isolated, absolute)
		}
	}
	return project, isolated
}
