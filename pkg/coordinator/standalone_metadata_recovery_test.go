package coordinator_test

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStandaloneGroupRegistrationSurvivesRestartWithoutCommit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.CreateTopic("events", 2, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2), "idempotent retry")
	cd.Stop()
	tm.Stop()
	dm.CloseAllHandlers()

	restartedDM, restartedTM, restarted := startStandaloneCoordinator(t, cfg, true)
	t.Cleanup(restartedDM.CloseAllHandlers)
	t.Cleanup(restarted.Stop)
	require.NotNil(t, restartedTM.GetTopic("events"))
	status, err := restarted.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, "events", status.TopicName)
	require.Equal(t, 2, status.PartitionCount)
	offset, found := restarted.GetOffset("workers", "events", 0)
	require.False(t, found)
	require.Zero(t, offset)
	commandHandler := controller.NewCommandHandler(restartedTM, cfg, restarted, nil, nil)
	t.Cleanup(func() { require.NoError(t, commandHandler.Close()) })
	require.Equal(t, "OK offset=0", commandHandler.HandleCommand(
		"FETCH_OFFSET topic=events partition=0 group=workers",
		controller.NewClientContext("", 0),
	))
	require.Equal(t, 1, restarted.RecoverySnapshot().RestoredGroups)
	observations := restarted.ObserveConsumerGroups()
	require.Len(t, observations, 1)
	require.Zero(t, observations[0].MemberCount)
	require.True(t, observations[0].LastActivity.IsZero(), "volatile heartbeat and lifecycle time is unknown after standalone recovery")
	require.True(t, observations[0].LastRebalance.IsZero(), "member assignments are not restored in standalone mode")
}

func TestStandaloneMultiTopicRegistrationSurvivesRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.CreateTopic("orders", 2, false, false))
	require.NoError(t, tm.CreateTopic("payments", 1, false, false))
	require.NoError(t, cd.RegisterGroupSubscription("workers", []string{"payments", "orders"}, "", map[string]int{"orders": 2, "payments": 1}))
	cd.Stop()
	tm.Stop()
	dm.CloseAllHandlers()

	restartedDM, _, restarted := startStandaloneCoordinator(t, cfg, true)
	t.Cleanup(restartedDM.CloseAllHandlers)
	t.Cleanup(restarted.Stop)
	status, err := restarted.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, []string{"orders", "payments"}, status.Topics)
	require.Equal(t, 3, status.PartitionCount)
}

func TestStandaloneOffsetAndTombstoneRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.CreateTopic("events", 2, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2))
	require.NoError(t, cd.CommitOffset("workers", "events", 0, 17))
	require.NoError(t, cd.CommitOffset("workers", "events", 1, 9))
	require.ErrorContains(t, cd.CommitOffset("workers", "events", 0, 16), "offset regression")
	cd.Stop()
	tm.Stop()
	dm.CloseAllHandlers()

	restartedDM, restartedTM, restarted := startStandaloneCoordinator(t, cfg, true)
	offset, found := restarted.GetOffset("workers", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(17), offset)
	require.ErrorContains(t, restarted.CommitOffset("workers", "events", 0, 16), "offset regression")
	require.NoError(t, restarted.DeleteGroup("workers"))
	restarted.Stop()
	restartedTM.Stop()
	restartedDM.CloseAllHandlers()

	finalDM, _, final := startStandaloneCoordinator(t, cfg, true)
	t.Cleanup(finalDM.CloseAllHandlers)
	t.Cleanup(final.Stop)
	require.Nil(t, final.GetGroup("workers"), "tombstone must fence legacy and offset records")
	require.Equal(t, 3, final.RecoverySnapshot().OrphanRecords, "registration and both offset snapshots are fenced by the tombstone")
}

func TestStandaloneCorruptConsumerRecordFailsRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.PublishWithAck(config.ConsumerOffsetsTopicName, &types.Message{Key: "corrupt", Payload: "not-json"}))
	cd.Stop()
	tm.Stop()
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	t.Cleanup(restartedDM.CloseAllHandlers)
	restartedTM := topic.NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restartedTM.RestoreTopics())
	t.Cleanup(restartedTM.Stop)
	restarted, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, restartedTM)
	require.ErrorContains(t, err, "decode internal metadata")
	t.Cleanup(restarted.Stop)
	require.Error(t, restarted.RecoveryReadinessError())
	require.False(t, restarted.RecoverySnapshot().Ready)
	require.Equal(t, 1, restarted.RecoverySnapshot().CorruptRecords)
}

func TestStandaloneTransactionOffsetReservationSurvivesDiskRestart(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "commit"}[committed], func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
			require.NoError(t, tm.CreateTopic("events", 2, false, false))
			require.NoError(t, cd.RegisterGroup("events", "workers", 2))
			_, err := cd.AddConsumer("workers", "worker")
			require.NoError(t, err)
			require.NoError(t, cd.CommitOffset("workers", "events", 0, 3))
			epoch := cd.GetRegistrationEpoch("workers")
			reservation := coordinator.TransactionOffsetReservation{
				TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: cd.GetGeneration("workers"),
				Offsets: []coordinator.ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 10}},
			}
			require.NoError(t, cd.PrepareOffsetReservation("workers", epoch, reservation))
			require.NoError(t, cd.RemoveConsumerForGeneration("workers", "worker", reservation.Generation))
			cd.Stop()
			tm.Stop()
			dm.CloseAllHandlers()

			restartedDM, restartedTM, restarted := startStandaloneCoordinator(t, cfg, true)
			require.Empty(t, restarted.GetGroup("workers").Members, "the old consumer departed before restart")
			_, _, err = restarted.GetStableOffset("workers", "events", 0)
			require.ErrorContains(t, err, "unstable_offset_commit", "the durable reservation must fence replacement consumers after restart")
			require.NoError(t, restarted.PrepareOffsetReservation("workers", epoch, reservation), "the existing authorization survives loss of membership")
			require.NoError(t, restarted.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, committed))
			restarted.Stop()
			restartedTM.Stop()
			restartedDM.CloseAllHandlers()

			_, _, final := startStandaloneCoordinator(t, cfg, true)
			offset, found, err := final.GetStableOffset("workers", "events", 0)
			require.NoError(t, err, "the release checkpoint must survive a second restart")
			require.True(t, found)
			want := uint64(3)
			if committed {
				want = 10
			}
			require.Equal(t, want, offset)
			require.Empty(t, final.GetGroup("workers").OffsetReservations)
			require.NoError(t, final.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, committed))
		})
	}
}

func TestDistributedReservationRecoveryPastUnresolvedTransaction(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	_, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.CreateTopic("events", 1, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 1))
	_, err := cd.AddConsumer("workers", "worker")
	require.NoError(t, err)
	require.NoError(t, cd.CommitOffset("workers", "events", 0, 3))
	reservation := coordinator.TransactionOffsetReservation{
		TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: cd.GetGeneration("workers"),
		Offsets: []coordinator.ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 10}},
	}
	// Unrelated unresolved transactional records pin the ordinary read_committed
	// view. Later nontransactional metadata is still replication-committed.
	for partition := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		require.NoError(t, tm.PublishToPartitionWithAck(config.ConsumerOffsetsTopicName, partition, &types.Message{
			Key: "unresolved", Payload: "pending", ProducerID: "pending-producer", SeqNum: 1,
			TransactionalID: "unresolved", TransactionState: types.TransactionStateOpen,
		}))
	}
	require.NoError(t, cd.PrepareOffsetReservation("workers", cd.GetRegistrationEpoch("workers"), reservation))
	require.NoError(t, cd.RemoveConsumerForGeneration("workers", "worker", reservation.Generation))
	cd.Stop()
	distributed := *cfg
	distributed.EnabledDistribution = true
	recovered, err := coordinator.NewCoordinatorWithRecovery(context.Background(), &distributed, tm)
	t.Cleanup(recovered.Stop)
	require.NoError(t, err)
	require.Empty(t, recovered.GetGroup("workers").Members, "later durable membership must not disappear behind an unrelated transaction")
	_, _, err = recovered.GetStableOffset("workers", "events", 0)
	require.ErrorContains(t, err, "unstable_offset_commit", "recovery must see the reservation beyond the last stable offset")
}

func startStandaloneCoordinator(t *testing.T, cfg *config.Config, restore bool) (*disk.DiskManager, *topic.TopicManager, *coordinator.Coordinator) {
	t.Helper()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	if restore {
		require.NoError(t, tm.RestoreTopics())
	}
	cd, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	tm.SetCoordinator(cd)
	t.Cleanup(func() {
		cd.Stop()
		tm.Stop()
		dm.CloseAllHandlers()
	})
	return dm, tm, cd
}
