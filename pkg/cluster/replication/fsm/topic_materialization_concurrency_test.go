package fsm

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestDefinitionMaterializationPreservesInFlightReplication(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(manager.Stop)
	state := NewBrokerFSM(manager, nil)
	registerLifecycleBroker(t, state, "broker-1", TopicLifecycleProtocolVersion)
	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 2}))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	release := partition.BeginReplicationMutation()
	defer release()
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "in-flight"}}))
	done := make(chan interface{}, 1)
	go func() {
		done <- state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 3})
	}()
	select {
	case result := <-done:
		require.Nil(t, result)
	case <-time.After(time.Second):
		t.Fatal("definition application blocked the FSM behind replication ownership; the queued HWM cannot apply")
	}
	require.Equal(t, uint64(1), partition.NextOffset(), "definition application must preserve the in-flight tail")
	require.Zero(t, partition.GetHWM(), "uncommitted data stays hidden")
	require.NoError(t, partition.ApplyReplicaHWM(1))
	require.Equal(t, uint64(1), partition.GetHWM())
}
