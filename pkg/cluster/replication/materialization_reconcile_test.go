package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type loopRecoverableHandlerProvider struct{ fail atomic.Bool }

func (p *loopRecoverableHandlerProvider) GetHandler(string, int) (types.StorageHandler, error) {
	if p.fail.Load() {
		return nil, errors.New("injected disk failure")
	}
	return &MockStorageHandler{}, nil
}

func TestReconcileTopicMaterializationsLoopRetriesAndStops(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	provider := &loopRecoverableHandlerProvider{}
	provider.fail.Store(true)
	manager := topic.NewTopicManager(cfg, provider, nil)
	brokerFSM := fsm.NewBrokerFSM(manager, nil)

	broker, err := json.Marshal(fsm.BrokerInfo{ID: "broker-1", Addr: "127.0.0.1:9000", Status: "active"})
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", broker)), Index: 1}))
	command, err := json.Marshal(fsm.TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()})
	require.NoError(t, err)
	require.Error(t, brokerFSM.Apply(&raft.Log{Data: []byte("TOPIC:" + string(command)), Index: 2}).(error))

	rm := &RaftReplicationManager{fsm: brokerFSM, brokerID: "broker-1"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rm.reconcileTopicMaterializations(ctx, 5*time.Millisecond)
		close(done)
	}()

	provider.fail.Store(false)
	require.Eventually(t, func() bool {
		return brokerFSM.TopicMaterializationReadinessError() == nil && manager.GetTopic("orders") != nil
	}, time.Second, 10*time.Millisecond)
	snapshot := brokerFSM.TopicMaterializationRuntimeSnapshot()
	require.GreaterOrEqual(t, snapshot.AttemptsByOperation[fsm.TopicMaterializationCreate].Failure, uint64(1))
	require.GreaterOrEqual(t, snapshot.AttemptsByOperation[fsm.TopicMaterializationCreate].Success, uint64(1))

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop after context cancellation")
	}
}
