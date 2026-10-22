package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCleanupExplicitOffsetMatchesComposeAndClients(t *testing.T) {
	t.Setenv("CURSUS_CLEANUP_PORT_OFFSET", "20000")
	t.Setenv("CURSUS_E2E_PORT_SHIFT", "40000")
	project, files := isolateCleanupCompose(t, "cursus-cleanup-e2e", []string{
		composeFile, "docker-compose.consumer-cleanup.yml", "docker-compose.consumer-cleanup-recovery.yml", faultComposeFile,
	})
	require.Equal(t, "cursus-truncate-20000-e2e", project)
	require.Len(t, files, 3)
	data, err := os.ReadFile(filepath.Clean(files[0]))
	require.NoError(t, err)
	var compose struct {
		Services map[string]struct {
			ContainerName string   `yaml:"container_name"`
			Ports         []string `yaml:"ports"`
			Environment   []string `yaml:"environment"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(data, &compose))
	for node := 1; node <= 3; node++ {
		service := compose.Services[fmt.Sprintf("broker-%d", node)]
		require.Equal(t, cleanupContainerName(node), service.ContainerName)
		require.Contains(t, service.Ports, fmt.Sprintf("%d:9000", brokerPort(node)))
		require.Contains(t, service.Ports, fmt.Sprintf("%d:9080", healthPort(node)))
		require.Contains(t, service.Ports, fmt.Sprintf("%d:9100", baseExporterPort+clusterPortShift()+node))
		require.Contains(t, service.Environment, fmt.Sprintf("ADVERTISED_BROKER_PORT=%d", brokerPort(node)))
	}
	recovery, err := os.ReadFile(filepath.Clean(files[1]))
	require.NoError(t, err)
	require.Contains(t, string(recovery), "image: "+project+":local")
	faults, err := os.ReadFile(filepath.Clean(files[2]))
	require.NoError(t, err)
	for node := 1; node <= 3; node++ {
		require.Contains(t, string(faults), fmt.Sprintf(`"%d:9100"`, baseExporterPort+clusterPortShift()+node))
		require.NotContains(t, string(faults), fmt.Sprintf(`"%d:9100"`, baseExporterPort+node))
	}
	require.NotContains(t, string(faults), "context: ../..")
}

func TestCleanupMetricReaderUsesIsolatedPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("cleanup_test_identity 42\n"))
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	t.Setenv("CURSUS_CLEANUP_PORT_OFFSET", "")
	t.Setenv("CURSUS_E2E_PORT_SHIFT", strconv.Itoa(port-baseExporterPort-1))
	value, err := fetchMetric(1, "cleanup_test_identity")
	require.NoError(t, err)
	require.Equal(t, float64(42), value, "ISR readiness must query the isolated cluster, not another cluster's default port")
}

// Separate host ports as well as containers, volumes and network from other
// local E2E work. Keep the same broker source, internal ports and test sequence.
func givenCleanupCluster(t *testing.T, files ...string) *ClusterTestContext {
	t.Helper()
	// The explicit offset uses main's compose isolation, including its custom
	// container names. Do not rewrite those ports a second time here.
	if cleanupPortOffset() != 0 {
		return givenClusterRestart(t, "cursus-cleanup-e2e", files...)
	}
	const shift = 40000
	t.Setenv("CURSUS_E2E_PORT_SHIFT", strconv.Itoa(shift))
	isolated := make([]string, 0, len(files))
	for _, name := range files {
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		var compose map[string]any
		require.NoError(t, yaml.Unmarshal(data, &compose))
		services, ok := compose["services"].(map[string]any)
		require.True(t, ok)
		for _, raw := range services {
			service := raw.(map[string]any)
			if ports, ok := service["ports"].([]any); ok {
				publishedPorts := make([]any, 0, len(ports))
				for _, rawPort := range ports {
					parts := strings.Split(rawPort.(string), ":")
					require.GreaterOrEqual(t, len(parts), 2)
					// Replication and discovery stay on the isolated Docker
					// network; these scenarios need no host binding for either.
					if target := parts[len(parts)-1]; target == "19000" || target == "8000" {
						continue
					}
					published := len(parts) - 2
					port, err := strconv.Atoi(parts[published])
					require.NoError(t, err)
					parts[published] = strconv.Itoa(port + shift)
					publishedPorts = append(publishedPorts, strings.Join(parts, ":"))
				}
				service["ports"] = publishedPorts
			}
			if environment, ok := service["environment"].([]any); ok {
				for i, value := range environment {
					if text := value.(string); strings.HasPrefix(text, "ADVERTISED_BROKER_PORT=") {
						port, err := strconv.Atoi(strings.TrimPrefix(text, "ADVERTISED_BROKER_PORT="))
						require.NoError(t, err)
						environment[i] = "ADVERTISED_BROKER_PORT=" + strconv.Itoa(port+shift)
					}
				}
			}
		}
		// Keep relative build/config paths anchored to the original directory.
		file, err := os.CreateTemp(".", ".cleanup-compose-*.json")
		require.NoError(t, err)
		path := file.Name()
		t.Cleanup(func() { _ = os.Remove(path) })
		require.NoError(t, json.NewEncoder(file).Encode(compose))
		require.NoError(t, file.Close())
		isolated = append(isolated, path)
	}
	return givenClusterRestart(t, "cursus-cleanup-e2e", isolated...)
}
