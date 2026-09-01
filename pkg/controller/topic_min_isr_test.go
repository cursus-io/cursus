package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateAndMetadataExposeTopicMinInSyncReplicas(t *testing.T) {
	handler, manager := newTestHandler(t)
	response := handler.HandleCommand(
		"CREATE topic=orders partitions=1 replication_factor=1 min_in_sync_replicas=1",
		NewClientContext("", 0),
	)
	require.True(t, strings.HasPrefix(response, "OK "), response)
	require.Contains(t, response, "min_in_sync_replicas=1")
	require.Contains(t, response, "effective_min_in_sync_replicas=1")
	require.Equal(t, 1, *manager.GetTopic("orders").Policy.MinInSyncReplicas)

	response = handler.HandleCommand("METADATA topic=orders", NewClientContext("", 0))
	require.Contains(t, response, "min_in_sync_replicas=1")
	require.Contains(t, response, "effective_min_in_sync_replicas=1")
}

func TestCreateTopicMinInSyncReplicasFallbackAndValidation(t *testing.T) {
	handler, manager := newTestHandler(t)
	handler.Config.MinInSyncReplicas = 2

	response := handler.HandleCommand("CREATE topic=fallback partitions=1", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(response, "OK "), response)
	require.Nil(t, manager.GetTopic("fallback").Policy.MinInSyncReplicas)
	require.Contains(t, response, "min_in_sync_replicas=default")
	require.Contains(t, response, "effective_min_in_sync_replicas=2")

	for _, command := range []string{
		"CREATE topic=zero partitions=1 min_in_sync_replicas=0",
		"CREATE topic=negative partitions=1 min_in_sync_replicas=-1",
		"CREATE topic=too-large partitions=1 replication_factor=1 min_in_sync_replicas=2",
	} {
		response = handler.HandleCommand(command, NewClientContext("", 0))
		require.Contains(t, response, "ERROR: invalid_min_in_sync_replicas", command)
	}
	require.Nil(t, manager.GetTopic("zero"))
	require.Nil(t, manager.GetTopic("negative"))
	require.Nil(t, manager.GetTopic("too-large"))
}

func TestAlterTopicMinInSyncReplicasAndResetToBrokerDefault(t *testing.T) {
	handler, manager := newTestHandler(t)
	handler.Config.MinInSyncReplicas = 2
	ctx := NewClientContext("", 0)

	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "OK ")
	require.Contains(t, handler.HandleCommand("CREATE topic=audit partitions=1", ctx), "OK ")

	response := handler.HandleCommand("ALTER_TOPIC_CONFIG topic=orders min_in_sync_replicas=1", ctx)
	require.True(t, strings.HasPrefix(response, "OK "), response)
	require.Equal(t, 1, *manager.GetTopic("orders").Policy.MinInSyncReplicas)
	require.Nil(t, manager.GetTopic("audit").Policy.MinInSyncReplicas)

	response = handler.HandleCommand("ALTER_TOPIC_CONFIG topic=orders min_in_sync_replicas=default", ctx)
	require.True(t, strings.HasPrefix(response, "OK "), response)
	require.Nil(t, manager.GetTopic("orders").Policy.MinInSyncReplicas)
	require.Contains(t, response, "effective_min_in_sync_replicas=2")
}

func TestRepeatedCreateWithoutMinInSyncReplicasPreservesOverride(t *testing.T) {
	handler, manager := newTestHandler(t)
	ctx := NewClientContext("", 0)
	require.Contains(t, handler.HandleCommand(
		"CREATE topic=orders partitions=1 replication_factor=1 min_in_sync_replicas=1", ctx,
	), "OK ")

	response := handler.HandleCommand("CREATE topic=orders partitions=1 retention_hours=24", ctx)
	require.True(t, strings.HasPrefix(response, "OK "), response)
	require.Equal(t, 1, *manager.GetTopic("orders").Policy.MinInSyncReplicas)
}
