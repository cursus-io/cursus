package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type coordinatorRoutingRaftManager struct {
	MockRaftManagerForForward
	brokerFSM *fsm.BrokerFSM
}

type coordinatorRoutingTopicHandler struct {
	coordinator.TopicHandler
}

func (coordinatorRoutingTopicHandler) CreateTopic(string, int, bool, bool) error {
	return nil
}

func (m *coordinatorRoutingRaftManager) GetFSM() *fsm.BrokerFSM {
	return m.brokerFSM
}

func newCoordinatorRoutingHandler(
	brokerID string,
	brokerFSM *fsm.BrokerFSM,
	groupCoordinator *coordinator.Coordinator,
) *CommandHandler {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	rm := &coordinatorRoutingRaftManager{brokerFSM: brokerFSM}
	rm.leaderAddress.Store("127.0.0.1:7000")
	router := clusterController.NewClusterRouter(
		brokerID,
		"127.0.0.1:7000",
		nil,
		rm,
		cfg.BrokerPort,
		cfg.AdvertisedClientHost,
		cfg,
	)
	cluster := &clusterController.ClusterController{RaftManager: rm, Router: router}
	return NewCommandHandler(nil, cfg, groupCoordinator, nil, cluster)
}

func registerRoutingBroker(t *testing.T, brokerFSM *fsm.BrokerFSM, id string) {
	t.Helper()
	result := brokerFSM.Apply(&raft.Log{
		Index: 1,
		Data:  []byte(`REGISTER:{"id":"` + id + `","addr":"127.0.0.1:7000","status":"active"}`),
	})
	require.Nil(t, result)
}

func TestCheckCoordinatorNormalLocalAndRemoteRoutes(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, brokerFSM, "node-1")

	t.Run("local", func(t *testing.T) {
		handler := newCoordinatorRoutingHandler("node-1", brokerFSM, nil)
		addr, isCoordinator, err := handler.checkCoordinator("workers")
		require.NoError(t, err)
		require.True(t, isCoordinator)
		require.Equal(t, AdvertisedAddr{}, addr)
	})

	t.Run("remote cached address", func(t *testing.T) {
		handler := newCoordinatorRoutingHandler("node-2", brokerFSM, nil)
		expected := AdvertisedAddr{Host: "broker-1.example", Port: 9100}
		handler.coordCache["node-1"] = coordCacheEntry{addr: expected, updated: time.Now()}

		addr, isCoordinator, err := handler.checkCoordinator("workers")
		require.NoError(t, err)
		require.False(t, isCoordinator)
		require.Equal(t, expected, addr)
	})
}

func TestResolveGroupCoordinatorAcrossThreeBrokersAndMovement(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	brokerIDs := []string{"node-1", "node-2", "node-3"}
	for _, brokerID := range brokerIDs {
		registerRoutingBroker(t, brokerFSM, brokerID)
	}

	handlers := make(map[string]*CommandHandler, len(brokerIDs))
	for _, brokerID := range brokerIDs {
		handlers[brokerID] = newCoordinatorRoutingHandler(brokerID, brokerFSM, nil)
	}
	groupName := "distributed-observation-group"
	ownerID, _, err := handlers[brokerIDs[0]].Cluster.Router.FindCoordinator(groupName)
	require.NoError(t, err)
	require.Equal(t, 1, resolvedCoordinatorCount(t, handlers, groupName))
	require.True(t, mustResolveGroupCoordinator(t, handlers[ownerID], groupName))

	result := brokerFSM.Apply(&raft.Log{
		Index: 2,
		Data:  []byte(`DEREGISTER:{"id":"` + ownerID + `"}`),
	})
	require.Nil(t, result)
	newOwnerID, _, err := handlers[brokerIDs[0]].Cluster.Router.FindCoordinator(groupName)
	require.NoError(t, err)
	require.NotEqual(t, ownerID, newOwnerID)
	require.Equal(t, 1, resolvedCoordinatorCount(t, handlers, groupName))
	require.True(t, mustResolveGroupCoordinator(t, handlers[newOwnerID], groupName))
	require.False(t, mustResolveGroupCoordinator(t, handlers[ownerID], groupName))
}

func TestResolveGroupCoordinatorsFailsClosedWithoutCurrentLeader(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, brokerFSM, "node-1")
	handler := newCoordinatorRoutingHandler("node-1", brokerFSM, nil)
	rm := handler.Cluster.RaftManager.(*coordinatorRoutingRaftManager)
	rm.leaderAddress.Store("")

	resolved, err := handler.ResolveGroupCoordinators([]string{"workers"})
	require.ErrorContains(t, err, "cluster leader unavailable")
	require.Nil(t, resolved)
}

func TestResolveGroupCoordinatorsFailsClosedWhenDistributedControlPlaneIsMissing(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	handler := NewCommandHandler(nil, cfg, nil, nil, nil)

	resolved, err := handler.ResolveGroupCoordinators([]string{"workers"})
	require.ErrorContains(t, err, "raft manager unavailable")
	require.Nil(t, resolved)
}

func resolvedCoordinatorCount(t *testing.T, handlers map[string]*CommandHandler, groupName string) int {
	t.Helper()
	count := 0
	for _, handler := range handlers {
		if mustResolveGroupCoordinator(t, handler, groupName) {
			count++
		}
	}
	return count
}

func mustResolveGroupCoordinator(t *testing.T, handler *CommandHandler, groupName string) bool {
	t.Helper()
	resolved, err := handler.ResolveGroupCoordinator(groupName)
	require.NoError(t, err)
	return resolved
}

func TestGroupCommandsFailClosedWhenCoordinatorDiscoveryFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	_, err := groupCoordinator.AddConsumer("workers", "worker-1")
	require.NoError(t, err)

	member := groupCoordinator.GetGroup("workers").Members["worker-1"]
	lastHeartbeat := member.LastHeartbeat
	handler := newCoordinatorRoutingHandler("node-2", nil, groupCoordinator)

	_, isCoordinator, err := handler.checkCoordinator("workers")
	require.ErrorContains(t, err, "FSM not available")
	require.False(t, isCoordinator)

	commands := []string{
		"JOIN_GROUP topic=orders group=workers member=new-worker",
		"SYNC_GROUP topic=orders group=workers member=worker-1",
		"LEAVE_GROUP topic=orders group=workers member=worker-1 generation=1",
		"FETCH_OFFSET topic=orders partition=0 group=workers",
		"GROUP_STATUS group=workers",
		"HEARTBEAT topic=orders group=workers member=worker-1",
		"COMMIT_OFFSET topic=orders partition=0 group=workers offset=1",
		"BATCH_COMMIT topic=orders group=workers generation=1 member=worker-1 offsets=P0:1",
	}
	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			response := handler.HandleCommand(command, NewClientContext("workers", 0))
			require.Equal(t, coordinatorUnavailableResponse, response)
		})
	}

	require.Equal(t, lastHeartbeat, member.LastHeartbeat)
}

func TestDistributedOffsetCommandsMatchStrictRaftSchema(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 2))
	_, err := groupCoordinator.AddConsumer("workers", "worker-1")
	require.NoError(t, err)
	generation := groupCoordinator.GetGeneration("workers")

	brokerFSM := fsm.NewBrokerFSM(nil, groupCoordinator)
	registerRoutingBroker(t, brokerFSM, "node-1")
	raftManager := &coordinatorRoutingRaftManager{brokerFSM: brokerFSM}
	raftManager.state = brokerFSM
	raftManager.isLeader = true
	raftManager.leaderAddress.Store("127.0.0.1:7000")
	router := clusterController.NewClusterRouter(
		"node-1", "127.0.0.1:7000", nil, raftManager,
		cfg.BrokerPort, cfg.AdvertisedClientHost, cfg,
	)
	handler := NewCommandHandler(nil, cfg, groupCoordinator, nil, &clusterController.ClusterController{
		RaftManager: raftManager,
		Router:      router,
	})
	requestContext := NewClientContext("workers", 0)

	response := handler.HandleCommand(fmt.Sprintf(
		"COMMIT_OFFSET topic=orders partition=0 group=workers offset=5 member=worker-1 generation=%d",
		generation,
	), requestContext)
	require.Equal(t, "OK", response)

	response = handler.HandleCommand(fmt.Sprintf(
		"BATCH_COMMIT topic=orders group=workers member=worker-1 generation=%d offsets=P0:6,P1:7",
		generation,
	), requestContext)
	require.Equal(t, "OK batched=2", response)
	offset, ok := groupCoordinator.GetOffset("workers", "orders", 1)
	require.True(t, ok)
	require.Equal(t, uint64(7), offset)

	legacyPositional := handler.HandleCommand(fmt.Sprintf(
		"BATCH_COMMIT topic=orders group=workers member=worker-1 generation=%d P0:8",
		generation,
	), requestContext)
	require.Equal(t, "ERROR: invalid_batch_commit_format", legacyPositional)
}
