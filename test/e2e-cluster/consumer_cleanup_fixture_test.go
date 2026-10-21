package e2e_cluster

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Separate host ports as well as containers, volumes and network from other
// local E2E work. Keep the same broker source, internal ports and test sequence.
func givenCleanupCluster(t *testing.T, files ...string) *ClusterTestContext {
	t.Helper()
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
