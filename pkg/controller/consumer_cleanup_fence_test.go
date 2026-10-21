package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestConsumerTombstoneWriterRejectsFollowerAndUnrecoveredOwner(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, state, "node-1")
	registerRoutingBroker(t, state, "node-2")
	installRoutingOffsetsTopology(t, state, "node-1")
	for _, broker := range []string{"node-1", "node-2"} {
		t.Run(broker, func(t *testing.T) {
			handler := newCoordinatorRoutingHandler(broker, state, nil)
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			manager := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
			t.Cleanup(manager.Stop)
			require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 4, false, false))
			handler.TopicManager = manager
			_, _, partition, epoch, err := handler.Cluster.Router.FindCoordinatorWithEpoch("workers")
			require.NoError(t, err)
			if broker == "node-2" {
				// A cached recovery epoch is not proof of current ownership.
				handler.groupRecoveryEpoch = map[uint64]int{partition: epoch}
			}
			err = handler.writeConsumerOffsetRecord(coordinator.ConsumerMetadataRecord{
				Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone,
				Group: "workers", Topic: "orders", Epoch: 2,
			})
			if broker == "node-1" {
				require.ErrorContains(t, err, "recovery pending")
			} else {
				require.ErrorContains(t, err, "not the current coordinator")
			}
			for _, p := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
				require.Zero(t, p.NextOffset(), "rejection must not append or forward a new tombstone")
			}
		})
	}
}
