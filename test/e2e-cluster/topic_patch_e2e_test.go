package e2e_cluster

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestRepeatedCreatePatchPreservesDefinitionAcrossThreeNodes(t *testing.T) {
	const topicName = "topic-patch-contract"
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic(topicName)
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()

	initial := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
		"CREATE topic="+topicName+" partitions=2 replication_factor=3 idempotent=true event_sourcing=false cleanup_policy=delete retention_hours=168 retention_bytes=1024 partitioner=round_robin auth_policy=acl read_acl=reader write_acl=writer",
	)
	requireDefinitionFields(t, initial, map[string]string{
		"topic": topicName, "partitions": "2", "revision": "1", "replication_factor": "3",
		"idempotent": "true", "event_sourcing": "false", "retention_hours": "168",
		"retention_bytes": "1024", "read_acl": "reader", "write_acl": "writer",
	})

	minimal := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "CREATE topic="+topicName)
	requireDefinitionFields(t, minimal, map[string]string{
		"revision": "1", "partitions": "2", "replication_factor": "3", "idempotent": "true",
		"retention_hours": "168", "retention_bytes": "1024", "read_acl": "reader", "write_acl": "writer",
	})

	commands := []struct {
		addr    string
		command string
	}{
		{ctx.GetBrokerAddrs()[0], "CREATE topic=" + topicName + " retention_bytes=8192"},
		{ctx.GetBrokerAddrs()[1], "CREATE topic=" + topicName + " write_acl=writer-2"},
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(commands))
	for _, item := range commands {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := e2e.NewBrokerClient([]string{item.addr})
			defer client.Close()
			response, err := client.SendCommand("admin", item.command, 10*time.Second)
			if err != nil {
				errs <- err
				return
			}
			if strings.HasPrefix(response, "ERROR:") {
				errs <- fmt.Errorf("%s", response)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
		"revision": "3", "partitions": "2", "replication_factor": "3", "idempotent": "true",
		"retention_hours": "168", "retention_bytes": "8192", "read_acl": "reader", "write_acl": "writer-2",
	})

	explicitZero := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
		"CREATE topic="+topicName+" retention_hours=0 read_acl=",
	)
	requireDefinitionFields(t, explicitZero, map[string]string{
		"revision": "4", "retention_hours": "0", "retention_bytes": "8192", "read_acl": "", "write_acl": "writer-2",
	})

	conflict := sendClusterTopicCommandRaw(t, ctx.GetBrokerAddrs(), "CREATE topic="+topicName+" idempotent=false")
	require.Contains(t, conflict, "ERROR: create_topic_failed")
	require.Contains(t, conflict, "idempotent mode is immutable")

	follower := waitForRaftFollower(t, actions)
	actions.StopBroker(follower)
	actions.StartBroker(follower)
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
		"revision": "4", "partitions": "2", "replication_factor": "3", "idempotent": "true",
		"retention_hours": "0", "retention_bytes": "8192", "read_acl": "", "write_acl": "writer-2",
	})
}

func sendClusterTopicCommand(t *testing.T, addrs []string, command string) string {
	t.Helper()
	response := sendClusterTopicCommandRaw(t, addrs, command)
	require.False(t, strings.HasPrefix(response, "ERROR:"), response)
	return response
}

func sendClusterTopicCommandRaw(t *testing.T, addrs []string, command string) string {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("admin", command, 10*time.Second)
	require.NoError(t, err)
	return response
}

func requireClusterDefinitionEventually(t *testing.T, addrs []string, topicName string, expected map[string]string) {
	t.Helper()
	require.NoError(t, eventually(t, "topic definition on all brokers", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", "METADATA topic="+topicName, 5*time.Second)
			client.Close()
			if err != nil {
				return false, addr + ": request failed", nil
			}
			fields := topicResponseFields(response)
			for key, value := range expected {
				if fields[key] != value {
					return false, fmt.Sprintf("%s: %s=%q want %q (%s)", addr, key, fields[key], value, response), nil
				}
			}
		}
		return true, "definition converged", nil
	}))
}

func requireDefinitionFields(t *testing.T, response string, expected map[string]string) {
	t.Helper()
	fields := topicResponseFields(response)
	for key, value := range expected {
		require.Equal(t, value, fields[key], "%s in %s", key, response)
	}
}

func topicResponseFields(response string) map[string]string {
	fields := make(map[string]string)
	for _, field := range strings.Fields(response) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			fields[key] = value
		}
	}
	return fields
}
