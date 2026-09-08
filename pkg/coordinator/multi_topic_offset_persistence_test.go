package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMultiTopicOffsetCommitKeepsSuccessfulSnapshotRevisionOnLaterWriteFailure(t *testing.T) {
	c := NewCoordinator(context.Background(), &config.Config{EnabledDistribution: true}, &DummyPublisher{})
	t.Cleanup(c.Stop)
	require.NoError(t, c.RegisterGroupSubscription(
		"workers",
		[]string{"payments", "orders"},
		"",
		map[string]int{"orders": 1, "payments": 1},
	))
	_, err := c.AddConsumer("workers", "worker-1")
	require.NoError(t, err)

	writes := 0
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		writes++
		if record.Topic == "payments" {
			return errors.New("injected payments snapshot failure")
		}
		return nil
	})

	group := c.GetGroup("workers")
	require.NotNil(t, group)
	err = c.ValidateAndCommitTopicOffsetsBulk(
		"workers",
		"worker-1",
		group.Generation,
		map[string][]OffsetItem{
			"orders":   {{Partition: 0, Offset: 10}},
			"payments": {{Partition: 0, Offset: 20}},
		},
	)
	require.ErrorContains(t, err, "injected payments snapshot failure")
	require.Equal(t, 2, writes)

	ordersOffset, found := c.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(10), ordersOffset)
	_, found = c.GetOffset("workers", "payments", 0)
	require.False(t, found)

	group.mu.RLock()
	require.Equal(t, uint64(1), group.OffsetRevisions["orders"])
	require.Zero(t, group.OffsetRevisions["payments"])
	group.mu.RUnlock()
}
