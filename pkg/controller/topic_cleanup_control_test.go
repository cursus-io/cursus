package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTopicCleanupReplaysBrokerControlsFromDiskAfterSnapshot(t *testing.T) {
	for _, mode := range []transaction.Mode{transaction.ModeLegacy, transaction.ModeProcessingV1} {
		for _, marker := range []string{types.TransactionMarkerCommit, types.TransactionMarkerAbort} {
			t.Run(fmt.Sprintf("%s/%s", mode, marker), func(t *testing.T) {
				ch, tm, owner, _ := newDiskBackedTransactionHandler(t)
				prepareTransactionGroup(t, tm, owner, "orders", "workers", "stale-member")
				prepareTransactionGroup(t, tm, owner, "payments", "unrelated", "live-member")
				require.NoError(t, owner.CommitOffset("unrelated", "payments", 0, 29))
				stale := owner.ExportState()
				cfg := config.DefaultConfig()
				cfg.EnabledDistribution = true
				follower, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, tm)
				require.NoError(t, err)
				t.Cleanup(follower.Stop)
				// Use the real broker emitter and disk serialization, including
				// processing markers whose coordinator and producer epochs differ.
				tx := &transaction.Transaction{ID: "history", Epoch: 3, CoordinatorEpoch: 7, Mode: mode,
					Messages: []transaction.MessageOperation{{Topic: config.ConsumerOffsetsTopicName, Partition: 0}}}
				require.NoError(t, ch.appendTransactionMarkers(tx, marker))
				payload, key, err := coordinator.EncodeConsumerMetadataRecord(coordinator.ConsumerMetadataRecord{
					Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordTombstone,
					Group: "workers", Topic: "orders", Epoch: stale["workers"].RegistrationEpoch + 1,
				})
				require.NoError(t, err)
				require.NoError(t, tm.PublishToPartitionWithAck(config.ConsumerOffsetsTopicName, 0, &types.Message{Payload: string(payload), Key: key}))

				require.NoError(t, follower.ImportState(stale))
				require.Len(t, follower.TopicGroupReferences("orders"), 1)
				unrelated := follower.ExportState()["unrelated"]
				deleted, err := follower.ReconcileTopicGroups("orders")
				require.NoError(t, err)
				require.Equal(t, []string{"workers"}, deleted)
				require.Nil(t, follower.GetGroup("workers"))
				require.Equal(t, unrelated, follower.ExportState()["unrelated"])
				require.True(t, follower.ExportState()["workers"].Deleted)
			})
		}
	}
}
