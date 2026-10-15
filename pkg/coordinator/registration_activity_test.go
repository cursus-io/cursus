package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestDistributedRegistrationActivityMerge verifies that replay fills unknown
// activity while preserving an existing nonzero observation, older or newer.
func TestDistributedRegistrationActivityMerge(t *testing.T) {
	registeredAt := time.Unix(100, 0).UTC()
	for _, existingActivity := range []time.Time{{}, registeredAt.Add(-time.Minute), registeredAt.Add(time.Minute)} {
		t.Run(existingActivity.String(), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			handler := &metadataReplayHandler{}
			c, err := NewCoordinatorWithRecovery(context.Background(), cfg, handler)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
			require.NoError(t, c.RegisterGroup("orders", "workers", 2))
			group := c.GetGroup("workers")
			group.mu.Lock()
			group.LastActivity = existingActivity
			group.mu.Unlock()
			handler.messages = map[int][]types.Message{0: {encodedMetadataMessage(t, ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "orders", PartitionCount: 2, Epoch: 1, Timestamp: registeredAt,
			}, 0)}}
			require.NoError(t, c.ReloadDistributedConsumerMetadata())
			want := existingActivity
			if want.IsZero() {
				want = registeredAt
			}
			require.Equal(t, want, c.ExportState()["workers"].LastActivity)
		})
	}
}

// TestDistributedRegistrationActivityLifecyclePrecedence keeps lifecycle activity
// and its legacy fallbacks authoritative even when all lifecycle times are unknown.
func TestDistributedRegistrationActivityLifecyclePrecedence(t *testing.T) {
	registeredAt := time.Unix(100, 0).UTC()
	activity := registeredAt.Add(time.Second)
	rebalancedAt := registeredAt.Add(2 * time.Second)
	recordedAt := registeredAt.Add(3 * time.Second)
	for _, test := range []struct {
		name                                 string
		activity, rebalance, timestamp, want time.Time
	}{
		{"activity", activity, rebalancedAt, recordedAt, activity},
		{"legacy rebalance", time.Time{}, rebalancedAt, recordedAt, rebalancedAt},
		{"legacy timestamp", time.Time{}, time.Time{}, recordedAt, recordedAt},
		{"unknown lifecycle", time.Time{}, time.Time{}, time.Time{}, time.Time{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registration := ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1, Timestamp: registeredAt,
			}
			lifecycle := ConsumerMetadataRecord{
				Version: ConsumerMetadataRecordVersionLifecycle, Type: ConsumerMetadataRecordLifecycleSnapshot,
				Group: "workers", Epoch: 1, Revision: 1, Timestamp: test.timestamp,
				Lifecycle: &GroupLifecycleSnapshot{TopicName: "orders", Generation: 1, Partitions: []int{0}, LastActivity: test.activity, LastRebalance: test.rebalance},
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			c, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{
				0: {encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, lifecycle, 1)},
			}})
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			require.Equal(t, test.want, c.ExportState()["workers"].LastActivity)
		})
	}
}
