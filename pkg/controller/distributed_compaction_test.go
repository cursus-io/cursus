package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestDistributedCompactionGateRequiresCurrentProtocolAndCaughtUpISR(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	state := fsm.NewBrokerFSM(topicManager, nil)
	for index, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		payload, err := json.Marshal(fsm.BrokerInfo{
			ID: brokerID, Addr: fmt.Sprintf("127.0.0.1:%d", 9000+index), Status: "active",
			LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
		})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...), Index: uint64(index + 1)}))
	}
	definition := topic.DefaultDefinition("state", cfg)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	definition.Policy.CleanupPolicy = config.CleanupPolicyCompact
	committedHWMVersion := fsm.CommittedHWMVersionCurrent
	payload, err := json.Marshal(fsm.TopicCommand{
		Definition: &definition, CommittedHWMVersion: &committedHWMVersion,
	})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...), Index: 4}))
	applyPartitionMetadata(t, state, "state", 0, fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 3, CommittedHWMKnown: true,
		Replicas: []string{"broker-1", "broker-2", "broker-3"},
		ISR:      []string{"broker-1", "broker-2", "broker-3"}, PartitionCount: 1,
	})
	raftManager := &MockRaftManagerForForward{state: state}
	cluster := clusterController.NewClusterController(context.Background(), cfg, raftManager, nil, "broker-2", "127.0.0.1:9001")
	handler := NewCommandHandler(topicManager, cfg, nil, nil, cluster)
	t.Cleanup(func() {
		_ = handler.Close()
		diskManager.CloseAllHandlers()
	})

	allowed, reason := handler.distributedCompactionAllowed("state", 0)
	require.True(t, allowed)
	require.Empty(t, reason)

	payload, err = json.Marshal(fsm.BrokerInfo{
		ID: "broker-3", Addr: "127.0.0.1:9002", Status: "active",
		LifecycleProtocol: fsm.DistributedCompactionProtocolVersion - 1,
	})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...), Index: 5}))
	allowed, reason = handler.distributedCompactionAllowed("state", 0)
	require.False(t, allowed)
	require.Equal(t, "mixed_broker_protocol", reason)

	payload, err = json.Marshal(fsm.BrokerInfo{
		ID: "broker-3", Addr: "127.0.0.1:9002", Status: "active",
		LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
	})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...), Index: 6}))
	metadata := state.GetPartitionMetadata("state-0")
	metadata.CommittedHWM = 1
	applyPartitionMetadata(t, state, "state", 0, *metadata)
	allowed, reason = handler.distributedCompactionAllowed("state", 0)
	require.False(t, allowed)
	require.Equal(t, "replica_not_caught_up", reason)
}
