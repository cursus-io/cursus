package e2e_cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestRepeatedCreatePatchPreservesDefinitionAcrossThreeNodes(t *testing.T) {
	const (
		topicName         = "topic-patch-contract"
		truncateTopicName = "topic-truncate-contract"
	)
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
			if response != "OK" && !strings.HasPrefix(response, "OK ") {
				errs <- fmt.Errorf("unexpected response: %s", response)
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

	conflict := sendClusterTopicCommandError(t, ctx.GetBrokerAddrs(), "CREATE topic="+topicName+" idempotent=false")
	requireBrokerError(t, conflict, "create_topic_failed", "idempotent mode is immutable")

	follower := waitForRaftFollower(t, actions)
	actions.StopBroker(follower)
	actions.StartBroker(follower)
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
		"revision": "4", "partitions": "2", "replication_factor": "3", "idempotent": "true",
		"retention_hours": "0", "retention_bytes": "8192", "read_acl": "", "write_acl": "writer-2",
	})
	requireClusterLifecycleProtocolEventually(t, ctx.GetBrokerAddrs(), fsm.TopicLifecycleProtocolVersion)

	truncateCreated := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
		"CREATE topic="+truncateTopicName+" partitions=2 replication_factor=3 idempotent=true retention_hours=48 retention_bytes=4096",
	)
	requireDefinitionFields(t, truncateCreated, map[string]string{
		"topic": truncateTopicName, "revision": "1", "lifecycle_epoch": "1", "partitions": "2",
		"replication_factor": "3", "idempotent": "true", "retention_hours": "48", "retention_bytes": "4096",
	})
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, map[string]string{
		"revision": "1", "lifecycle_epoch": "1", "partitions": "2", "replication_factor": "3",
	})

	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	require.NoError(t, client.PublishIdempotentToPartition(truncateTopicName, "truncate-producer", 0, 1, 0, "before-truncate", "all", true))
	client.Close()
	requireClusterPartitionOffsetsEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, 0, false)

	truncated := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "TRUNCATE topic="+truncateTopicName+" expected_revision=1")
	requireDefinitionFields(t, truncated, map[string]string{
		"topic": truncateTopicName, "truncated": "true", "revision": "2", "lifecycle_epoch": "2", "leo": "0", "hwm": "0",
	})
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, map[string]string{
		"revision": "2", "lifecycle_epoch": "2", "partitions": "2", "replication_factor": "3",
		"idempotent": "true", "retention_hours": "48", "retention_bytes": "4096",
	})
	requireClusterPartitionOffsetsEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, 0, true)
	conflictingTruncate := sendClusterTopicCommandError(t, ctx.GetBrokerAddrs(), "TRUNCATE topic="+truncateTopicName+" expected_revision=1")
	requireBrokerError(t, conflictingTruncate, "topic_revision_conflict", "")

	actions.StopBroker(follower)
	actions.StartBroker(follower)
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, map[string]string{
		"revision": "2", "lifecycle_epoch": "2", "partitions": "2", "replication_factor": "3",
	})
	requireClusterPartitionOffsetsEventually(t, ctx.GetBrokerAddrs(), truncateTopicName, 0, true)

	client = e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	require.NoError(t, client.PublishIdempotentToPartition(truncateTopicName, "truncate-producer", 0, 1, 0, "after-truncate", "all", true))
	client.Close()
	truncateDeleted := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic="+truncateTopicName)
	requireDefinitionFields(t, truncateDeleted, map[string]string{"topic": truncateTopicName, "deleted": "true"})
	requireClusterTopicMissingEventually(t, ctx.GetBrokerAddrs(), truncateTopicName)

	deleted := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic="+topicName)
	requireDefinitionFields(t, deleted, map[string]string{"topic": topicName, "deleted": "true"})
	requireBrokerError(t, sendClusterTopicCommandError(t, ctx.GetBrokerAddrs(), "DELETE topic="+topicName), "topic_not_found", "")
	idempotentDelete := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(), "DELETE topic="+topicName+" if_exists=true")
	requireDefinitionFields(t, idempotentDelete, map[string]string{"topic": topicName, "deleted": "false"})
	requireBrokerError(t, sendClusterTopicCommandError(t, ctx.GetBrokerAddrs(), "DELETE topic=__consumer_offsets if_exists=true"), "internal_topic_delete_forbidden", "")
	requireClusterTopicMissingEventually(t, ctx.GetBrokerAddrs(), topicName)

	actions.StopBroker(follower)
	actions.StartBroker(follower)
	requireClusterTopicMissingEventually(t, ctx.GetBrokerAddrs(), topicName)

	recreated := sendClusterTopicCommand(t, ctx.GetBrokerAddrs(),
		"CREATE topic="+topicName+" partitions=1 replication_factor=3 retention_hours=24",
	)
	requireDefinitionFields(t, recreated, map[string]string{
		"topic": topicName, "revision": "1", "partitions": "1", "replication_factor": "3",
		"retention_hours": "24", "retention_bytes": "0", "read_acl": "", "write_acl": "",
	})
	requireClusterDefinitionEventually(t, ctx.GetBrokerAddrs(), topicName, map[string]string{
		"revision": "1", "partitions": "1", "replication_factor": "3",
		"retention_hours": "24", "retention_bytes": "0", "read_acl": "", "write_acl": "",
	})
}

func sendClusterTopicCommand(t *testing.T, addrs []string, command string) string {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("admin", command, 10*time.Second)
	require.NoError(t, err)
	return response
}

func sendClusterTopicCommandError(t *testing.T, addrs []string, command string) error {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	_, err := client.SendCommand("admin", command, 10*time.Second)
	require.Error(t, err)
	return err
}

func requireBrokerError(t *testing.T, err error, code, reason string) {
	t.Helper()
	var brokerErr *wire.BrokerError
	require.True(t, errors.As(err, &brokerErr), "%T %v", err, err)
	require.Equal(t, code, brokerErr.Code)
	if reason != "" {
		require.Contains(t, brokerErr.Fields["reason"], reason)
	}
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

func requireClusterTopicMissingEventually(t *testing.T, addrs []string, topicName string) {
	t.Helper()
	require.NoError(t, eventually(t, "topic deletion on all brokers", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", "METADATA topic="+topicName, 5*time.Second)
			client.Close()
			if topicMissingResult(response, err) {
				continue
			}
			if err != nil {
				return false, fmt.Sprintf("%s: request failed: %v", addr, err), nil
			}
			return false, fmt.Sprintf("%s: topic still present (%s)", addr, response), nil
		}
		return true, "topic deletion converged", nil
	}))
}

func topicMissingResult(response string, err error) bool {
	var brokerErr *wire.BrokerError
	return errors.As(err, &brokerErr) && brokerErr.Code == "topic_not_found" ||
		strings.Contains(response, "topic_not_found")
}

func TestTopicMissingResultHandlesWireV2Error(t *testing.T) {
	require.True(t, topicMissingResult("", &wire.BrokerError{Code: "topic_not_found"}))
	require.True(t, topicMissingResult("ERROR: topic_not_found", nil))
	require.False(t, topicMissingResult("", errors.New("connection reset")))
}

func requireClusterPartitionOffsetsEventually(t *testing.T, addrs []string, topicName string, partition int, empty bool) {
	t.Helper()
	require.NoError(t, eventually(t, "partition offsets on all brokers", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", fmt.Sprintf("LIST_OFFSETS topic=%s partition=%d", topicName, partition), 5*time.Second)
			client.Close()
			if err != nil {
				return false, addr + ": request failed", nil
			}
			isEmpty := strings.Contains(response, "leo=0:hwm=0")
			if isEmpty != empty {
				return false, fmt.Sprintf("%s: unexpected offsets (%s)", addr, response), nil
			}
		}
		return true, "offsets converged", nil
	}))
}

func requireClusterLifecycleProtocolEventually(t *testing.T, addrs []string, minimum int) {
	t.Helper()
	require.NoError(t, eventually(t, "lifecycle protocol on all brokers", clusterReadyTimeout, func() (bool, string, error) {
		client := e2e.NewBrokerClient(addrs)
		response, err := client.SendCommand("", "LIST_CLUSTER", 5*time.Second)
		client.Close()
		if err != nil {
			return false, "LIST_CLUSTER request failed", nil
		}
		const prefix = "OK brokers="
		if !strings.HasPrefix(response, prefix) {
			return false, response, nil
		}
		var brokers []fsm.BrokerInfo
		if err := json.Unmarshal([]byte(strings.TrimPrefix(response, prefix)), &brokers); err != nil {
			return false, "invalid LIST_CLUSTER response", err
		}
		active := 0
		for _, broker := range brokers {
			if !strings.EqualFold(broker.Status, "active") {
				continue
			}
			active++
			if broker.LifecycleProtocol < minimum {
				return false, fmt.Sprintf("%s advertises lifecycle protocol %d", broker.ID, broker.LifecycleProtocol), nil
			}
		}
		return active == len(addrs), fmt.Sprintf("%d/%d active brokers advertise lifecycle protocol %d", active, len(addrs), minimum), nil
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
