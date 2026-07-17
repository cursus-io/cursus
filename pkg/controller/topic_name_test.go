package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopicCommandsRejectNonPortableNames(t *testing.T) {
	handler, manager := newTestHandler(t)
	context := NewClientContext("default-group", 0)

	for _, command := range []string{
		"CREATE topic=../escape partitions=1",
		`CREATE topic=orders\\2026 partitions=1`,
		"DELETE topic=../escape",
	} {
		response := handler.HandleCommand(command, context)
		require.True(t, strings.HasPrefix(response, "ERROR: invalid_topic_name"), response)
	}
	require.Empty(t, manager.ListTopics())
}
