package controller

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTruncateInstanceRetryPreservesNewGroupAndRejectsRecreation(t *testing.T) {
	handler, manager, groups := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)
	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1", ctx), "instance_id=")
	id := manager.GetTopic("orders").Definition().InstanceID
	command := fmt.Sprintf("TRUNCATE_INSTANCE topic=orders expected_revision=1 expected_instance=%s", id)
	require.Contains(t, handler.HandleCommand(command, ctx), "revision=2 lifecycle_epoch=2")
	require.NoError(t, groups.RegisterGroupSubscription("new-workers", []string{"orders", "audit"}, "", map[string]int{"orders": 1, "audit": 1}))
	require.NoError(t, groups.CommitOffset("new-workers", "orders", 0, 7))
	before := groups.ExportState()
	require.Contains(t, handler.HandleCommand(command, ctx), "replayed=true")
	require.Equal(t, before, groups.ExportState(), "receipt replay must not run dependency cleanup")
	require.Contains(t, handler.HandleCommand("TRUNCATE_INSTANCE topic=orders expected_revision=2 expected_instance="+id, ctx), "topic_truncate_blocked")
	require.Contains(t, handler.HandleCommand("TRUNCATE_INSTANCE topic="+config.ConsumerOffsetsTopicName+" expected_revision=1 expected_instance="+id, ctx), "internal_topic_truncate_forbidden")
	require.Contains(t, handler.HandleCommand("TRUNCATE_INSTANCE topic=orders expected_revision=1", ctx), "invalid_expected_instance")
}
