package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerRecoveryWriterRecordsActualRouteBeforeFailedPublish(t *testing.T) {
	for _, routed := range []bool{true, false} {
		t.Run(fmt.Sprint(routed), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = true
			tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
			t.Cleanup(tm.Stop)
			require.NoError(t, tm.CreateTopic(config.ConsumerOffsetsTopicName, 5, false, false))
			c := coordinator.NewCoordinator(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
			t.Cleanup(c.Stop)
			brokerFSM := fsm.NewBrokerFSM(nil, nil)
			registerRoutingBroker(t, brokerFSM, "node-1")
			installRoutingOffsetsTopology(t, brokerFSM, "node-1")
			h := newCoordinatorRoutingHandler("node-1", brokerFSM, c)
			h.TopicManager = tm
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			if !routed {
				h.Cluster.Router = nil
			}
			internal := tm.GetTopic(config.ConsumerOffsetsTopicName)
			group := ""
			expected := 0
			for n := 0; n < 100; n++ {
				candidate := fmt.Sprintf("route-%d", n)
				fallback := internal.GetPartitionForMessage(types.Message{Key: coordinator.ConsumerMetadataGroupPartitionKey(candidate)})
				if !routed {
					group = candidate
					expected = fallback
					break
				}
				_, _, partition, _, err := h.Cluster.Router.FindCoordinatorWithEpoch(candidate)
				require.NoError(t, err)
				if int(partition) != fallback {
					group = candidate
					expected = int(partition)
					break
				}
			}
			require.NotEmpty(t, group)
			// Force a policy rejection after route resolution, before storage/network IO.
			internal.Policy.AggregateReplay = true
			err := h.writeConsumerOffsetRecord(coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: group, Topic: "events", PartitionCount: 1, Epoch: 1})
			require.Error(t, err)
			_, proof, err := c.ExportStateWithRecovery()
			require.NoError(t, err)
			require.Equal(t, []int{expected}, proof.SourcePartitions[group])
			require.True(t, proof.KnownComplete)
		})
	}
}
