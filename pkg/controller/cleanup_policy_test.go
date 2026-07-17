package controller

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCreateAndMetadataExposeCleanupPolicy(t *testing.T) {
	handler, manager := newTestHandler(t)
	response := handler.HandleCommand("CREATE topic=state partitions=1 cleanup_policy=compact", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(response, "OK "))
	require.Contains(t, response, "cleanup_policy=compact")
	require.Equal(t, config.CleanupPolicyCompact, manager.GetTopic("state").Policy.CleanupPolicy)

	response = handler.HandleCommand("METADATA topic=state", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(response, "OK "))
	require.Contains(t, response, "cleanup_policy=compact")
}

func TestCreateRejectsCompactionForExistingEventSourcingTopic(t *testing.T) {
	handler, _ := newTestHandler(t)
	response := handler.HandleCommand("CREATE topic=events partitions=1 event_sourcing=true cleanup_policy=delete", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(response, "OK "))

	response = handler.HandleCommand("CREATE topic=events partitions=1 cleanup_policy=compact", NewClientContext("", 0))
	require.Contains(t, response, "ERROR: invalid_topic_policy")
}

func TestCreateRejectsCompactionForUnsupportedTopicModes(t *testing.T) {
	handler, _ := newTestHandler(t)
	response := handler.HandleCommand("CREATE topic=events partitions=1 event_sourcing=true cleanup_policy=compact", NewClientContext("", 0))
	require.Contains(t, response, "ERROR: invalid_topic_policy")

	handler.Config.EnabledDistribution = true
	response = handler.HandleCommand("CREATE topic=distributed partitions=1 cleanup_policy=compact", NewClientContext("", 0))
	require.Contains(t, response, "ERROR: unsupported_topic_policy")
}
