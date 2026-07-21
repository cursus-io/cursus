package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type runtimeFailingHandlerProvider struct{}

func (runtimeFailingHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return nil, errors.New("injected disk failure")
}

func TestRuntimeSnapshotIncludesTopicMaterializationState(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	topicManager := topic.NewTopicManager(cfg, runtimeFailingHandlerProvider{}, nil)
	brokerFSM := fsm.NewBrokerFSM(topicManager, nil)
	broker, err := json.Marshal(fsm.BrokerInfo{ID: "broker-1", Addr: "127.0.0.1:9000", Status: "active"})
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", broker)), Index: 1}))
	command, err := json.Marshal(fsm.TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()})
	require.NoError(t, err)
	require.Error(t, brokerFSM.Apply(&raft.Log{Data: []byte("TOPIC:" + string(command)), Index: 2}).(error))

	manager := &ComprehensiveMockRaftManager{isLeader: true, mockFSM: brokerFSM}
	manager.On("GetLeaderAddress").Return("broker-1:9001").Once()
	controller := &ClusterController{RaftManager: manager, brokerID: "broker-1"}

	snapshot := controller.RuntimeSnapshot()
	require.Equal(t, 1, snapshot.TopicMaterializationsPending[fsm.TopicMaterializationCreate])
	require.Equal(t, uint64(1), snapshot.TopicMaterializationAttempts[fsm.TopicMaterializationCreate].Failure)
	require.GreaterOrEqual(t, snapshot.TopicMaterializationOldestPending, float64(0))
}
