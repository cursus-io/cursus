package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestCommandHandler_DistributionDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = false
	ch := NewCommandHandler(nil, cfg, nil, nil, nil)

	// Should return false for forwarded
	resp, forwarded, err := ch.isLeaderAndForward("HELP")
	assert.Empty(t, resp)
	assert.False(t, forwarded)
	assert.NoError(t, err)

	resp, forwarded, err = ch.isCoordinatorAndForward("group1", "HELP")
	assert.Empty(t, resp)
	assert.False(t, forwarded)
	assert.NoError(t, err)
}

func TestIsTopicMatched(t *testing.T) {
	assert.True(t, isTopicMatched("test", "test"))
	assert.True(t, isTopicMatched("test*", "test-topic"))
	assert.True(t, isTopicMatched("*-topic", "test-topic"))
	assert.True(t, isTopicMatched("t?st", "test"))
	assert.False(t, isTopicMatched("test", "other"))
	assert.False(t, isTopicMatched("test*", "other"))
}

func TestParseKeyValueArgs(t *testing.T) {
	args := parseKeyValueArgs("topic=t1 partitions=4")
	assert.Equal(t, "t1", args["topic"])
	assert.Equal(t, "4", args["partitions"])

	args = parseKeyValueArgs("topic=t2 message=hello world with spaces")
	assert.Equal(t, "t2", args["topic"])
	assert.Equal(t, "hello world with spaces", args["message"])
}
