package controller

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestElectLeaderPopulatesTopologyFences(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for _, broker := range []fsm.BrokerInfo{
		{ID: "broker-1", Addr: "broker-1:9001", Status: "active"},
		{ID: "broker-2", Addr: "broker-2:9001", Status: "active"},
	} {
		encoded, err := json.Marshal(broker)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), encoded...)}))
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 2
	encoded, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), encoded...)}))
	metadata := fsm.PartitionMetadata{
		Leader: "broker-2", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		PartitionCount: 1, Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
	}
	encoded, err = json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + string(encoded))}))

	raftManager := &MockRaftManagerForForward{isLeader: true, state: state}
	raftManager.leaderAddress.Store("broker-2:9001")
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	handler := NewCommandHandler(nil, cfg, nil, nil, &clusterController.ClusterController{RaftManager: raftManager})
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	status := buildClusterStatus(state, "broker-2:9001", 1)
	require.Equal(t, 1, status.LeaderRebalanceImbalanced)
	require.Equal(t, 1, status.LeaderRebalanceEligible)
	require.Equal(t, 1, status.LeaderRebalanceSelected)
	require.Len(t, status.Partitions, 1)
	require.Equal(t, "broker-1", status.Partitions[0].PreferredLeader)
	require.True(t, status.Partitions[0].LeaderRebalanceEligible)
	require.Empty(t, status.Partitions[0].LeaderRebalanceExclusionReason)

	response := handler.handleElectLeader("ELECT_LEADER topic=orders partition=0 broker=broker-1")
	require.Contains(t, response, "OK topic=orders partition=0")
	require.Contains(t, response, "leader=broker-1")
	require.Equal(t, 5, state.GetPartitionMetadata("orders-0").LeaderEpoch)
}

func TestElectLeaderAllowsRemovingTheOldLeaderAfterReplicaCatchup(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	apply := func(prefix string, value any) {
		t.Helper()
		payload, err := json.Marshal(value)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte(prefix+":"), payload...)}))
	}
	for _, id := range []string{"broker-1", "broker-2", "broker-3"} {
		apply("REGISTER", fsm.BrokerInfo{
			ID: id, Addr: id + ":9001", Status: "active", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
		})
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 2
	definition.InstanceID = rand.Text()
	apply("TOPIC", fsm.TopicCommand{Definition: &definition})

	initial := state.GetPartitionMetadata("orders-0")
	require.Len(t, initial.Replicas, 2)
	replacement := ""
	for _, id := range []string{"broker-1", "broker-2", "broker-3"} {
		if !slices.Contains(initial.Replicas, id) {
			replacement = id
			break
		}
	}
	require.NotEmpty(t, replacement)
	manager := &MockRaftManagerForForward{isLeader: true, state: state}
	manager.leaderAddress.Store(initial.Leader + ":9001")
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	handler := NewCommandHandler(nil, cfg, nil, nil, &clusterController.ClusterController{RaftManager: manager})
	t.Cleanup(func() { require.NoError(t, handler.Close()) })

	expandedReplicas := append(slices.Clone(initial.Replicas), replacement)
	reassign := func(replicas []string) string {
		return handler.handleReassignPartition("REASSIGN_PARTITION topic=orders partition=0 replicas=" + strings.Join(replicas, ","))
	}
	require.Contains(t, reassign(expandedReplicas), "OK topic=orders")
	elect := "ELECT_LEADER topic=orders partition=0 broker=" + replacement
	require.Contains(t, handler.handleElectLeader(elect), "not in the ISR")

	caughtUp := state.GetPartitionMetadata("orders-0")
	caughtUp.ISR = slices.Clone(expandedReplicas)
	apply("PARTITION:orders-0", caughtUp)
	recoveryWork := func() string {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() == "cursus_recovery_work_total" {
				return family.String()
			}
		}
		return ""
	}
	workBefore := recoveryWork()
	response := handler.handleElectLeader(elect)
	require.Contains(t, response, fmt.Sprintf(" leader=%s leader_epoch=%d changed=true", replacement, initial.LeaderEpoch+1))
	caughtUp.Leader = replacement
	caughtUp.LeaderEpoch++
	require.Equal(t, caughtUp, state.GetPartitionMetadata("orders-0"))
	require.Equal(t, workBefore, recoveryWork(), "leader election must not add recovery work or bytes")

	finalReplicas := slices.DeleteFunc(slices.Clone(expandedReplicas), func(id string) bool { return id == initial.Leader })
	response = reassign(finalReplicas)
	require.Contains(t, response, "OK topic=orders")
	require.Equal(t, finalReplicas, state.GetPartitionMetadata("orders-0").Replicas)
	require.Equal(t, replacement, state.GetPartitionMetadata("orders-0").Leader)
}

func TestClusterStatusExposesCommittedCleanupPendingOnWire(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	c := coordinator.NewCoordinator(context.Background(), cfg, &dummyPublisher{})
	t.Cleanup(func() { c.Stop(); manager.Stop() })
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return nil })
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	_, err := c.AddConsumer("workers", "still-live")
	require.NoError(t, err)
	state := fsm.NewBrokerFSM(manager, c)
	registerRoutingBroker(t, state, "local")
	definition := topic.DefaultDefinition("orders", cfg)
	definition.ReplicationFactor = 1
	create, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...)}))
	state.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`)})

	encoded, err := json.Marshal(buildClusterStatus(state, "local:9001", 1))
	require.NoError(t, err)
	var wire struct {
		Pending int                             `json:"topic_materialization_pending"`
		Issues  []fsm.TopicMaterializationIssue `json:"topic_materialization_issues"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Equal(t, 1, wire.Pending)
	require.Len(t, wire.Issues, wire.Pending)
	issue := wire.Issues[0]
	require.Equal(t, "orders", issue.Topic)
	require.Equal(t, "truncate", issue.Operation)
	require.Equal(t, uint64(2), issue.Revision)
	require.Equal(t, uint64(2), issue.LifecycleEpoch)
	require.NotNil(t, issue.ConsumerCleanup)
	require.Equal(t, "workers", issue.ConsumerCleanup.Group)
	require.NotEmpty(t, issue.ConsumerCleanup.Reason)
	require.NotNil(t, c.GetGroup("workers"), "status cannot authorize cleanup")
}

func TestBuildClusterStatusReportsAvailabilityAndReplication(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for _, broker := range []fsm.BrokerInfo{
		{ID: "broker-1", Addr: "broker-1:9001", Status: "active", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent},
		{ID: "broker-2", Addr: "broker-2:9001", Status: "active", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent},
		{ID: "broker-3", Addr: "broker-3:9001", Status: "active", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent},
	} {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatal(err)
		}
		state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)})
	}
	definition := topic.DefaultDefinition("orders-eu", nil)
	definition.Partitions = 2
	definition.ReplicationFactor = 3
	create, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...)}); result != nil {
		t.Fatalf("create topic: %v", result)
	}
	if result := state.Apply(&raft.Log{Data: []byte(`DEREGISTER:{"id":"broker-3"}`)}); result != nil {
		t.Fatalf("deregister broker: %v", result)
	}
	partitions := map[string]fsm.PartitionMetadata{
		"orders-eu-0": {
			Leader: "broker-1", LeaderEpoch: 2, CommittedHWM: 11,
			Replicas: []string{"broker-1", "broker-2", "broker-3"}, ISR: []string{"broker-1", "broker-2"},
			LifecycleEpoch: topic.InitialLifecycleEpoch,
		},
		"orders-eu-1": {
			Leader: "broker-3", LeaderEpoch: 4, CommittedHWM: 9,
			Replicas: []string{"broker-3"}, ISR: []string{"broker-3"},
			LifecycleEpoch: topic.InitialLifecycleEpoch,
		},
	}
	for key, metadata := range partitions {
		data, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		state.Apply(&raft.Log{Data: []byte("PARTITION:" + key + ":" + string(data))})
	}

	status := buildClusterStatus(state, "broker-1:9001", 2)
	if status.BrokerCount != 3 || status.ActiveBrokers != 2 || status.InactiveBrokers != 1 {
		t.Fatalf("unexpected broker status: %+v", status)
	}
	if status.PartitionCount != 2 || status.UnderReplicated != 2 || status.Leaderless != 1 {
		t.Fatalf("unexpected partition status: %+v", status)
	}
	if status.AssignmentDeficient != 1 || status.InactiveReplicaPartitions != 2 || status.MinISRUnsatisfied != 1 || status.Healthy {
		t.Fatalf("unexpected topology health: %+v", status)
	}
	if status.Partitions[0].Topic != "orders-eu" || status.Partitions[0].Partition != 0 {
		t.Fatalf("hyphenated topic was not parsed correctly: %+v", status.Partitions[0])
	}
}

func TestBuildClusterStatusFailsHealthForLocalReplicaMaterializationGap(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	t.Cleanup(topicManager.Stop)
	state := fsm.NewBrokerFSM(topicManager, nil)

	broker, err := json.Marshal(fsm.BrokerInfo{
		ID: "broker-1", Addr: "broker-1:9001", Status: "active", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
	})
	if err != nil {
		t.Fatal(err)
	}
	state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), broker...)})
	definition := topic.DefaultDefinition("orders", cfg)
	definition.ReplicationFactor = 1
	create, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...)}); result != nil {
		t.Fatalf("create topic: %v", result)
	}
	metadata := fsm.PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 1, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 1, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"broker-1"}, ISR: []string{"broker-1"},
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + string(encoded))}); result != nil {
		t.Fatalf("install partition metadata: %v", result)
	}
	if err := state.ReconcileReplicaMaterializations("broker-1"); err == nil {
		t.Fatal("expected local replica materialization gap")
	}

	status := buildClusterStatus(state, "broker-1:9001", 1)
	if status.ReplicaMaterializationPending != 1 || status.Healthy {
		t.Fatalf("local replica gap was hidden by topology health: %+v", status)
	}
}

func TestClusterAdminCommandsAreRegisteredOutsideDistributedMode(t *testing.T) {
	ch, _ := newTestHandler(t)
	for _, command := range []string{
		"CLUSTER_STATUS",
		"ELECT_LEADER topic=orders partition=0 broker=broker-2",
		"REASSIGN_PARTITION topic=orders partition=0 replicas=broker-1,broker-2,broker-3",
	} {
		resp := ch.HandleCommand(command, NewClientContext("", 0))
		if strings.Contains(resp, "unknown_command") || !strings.Contains(resp, "distribution_required") {
			t.Fatalf("%s was not routed to its handler: %s", command, resp)
		}
	}
}
