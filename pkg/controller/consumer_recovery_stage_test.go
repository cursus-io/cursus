package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type stagedConsumerTestReader struct {
	ends     []uint64
	closed   bool
	sources  map[int]ConsumerMetadataRecoverySource
	records  map[int][]types.Message
	readHook func()
	reads    int
	bytes    int
}

func (r *stagedConsumerTestReader) ExistingPartitionCount(string) (int, error) {
	return len(r.ends), nil
}
func (r *stagedConsumerTestReader) EarliestTopicOffset(string, int) (uint64, error) { return 0, nil }
func (r *stagedConsumerTestReader) EndTopicOffset(_ string, p int) (uint64, error) {
	return r.ends[p], nil
}
func (r *stagedConsumerTestReader) ReadCommittedConsumerMetadata(_ string, partition int, offset uint64, max int) ([]types.Message, error) {
	if r.readHook != nil {
		r.readHook()
	}
	r.reads++
	if max <= 0 {
		return nil, nil
	}
	for _, record := range r.records[partition] {
		if record.Offset >= offset && record.Offset < r.ends[partition] {
			r.bytes += len(record.Payload)
			// Deliberately short pages with a hole exercise strict pagination;
			// one page must not be mistaken for the end of the staged range.
			return []types.Message{record}, nil
		}
	}
	return nil, nil
}
func (r *stagedConsumerTestReader) Close() error { r.closed = true; return nil }
func (r *stagedConsumerTestReader) Sources() map[int]ConsumerMetadataRecoverySource {
	if r.sources != nil {
		return r.sources
	}
	return map[int]ConsumerMetadataRecoverySource{0: {BrokerID: "remote"}}
}

func TestConsumerBootstrapStagesNonlocalOtherPartitionBeforeLease(t *testing.T) {
	h, p, receiver, identity := consumerBootstrapFixture(t)
	limits, err := config.DefaultRecoveryConfig().Limits()
	require.NoError(t, err)
	budget, err := recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	h.SetRecoveryBudget(budget)
	state := h.Cluster.RaftManager.GetFSM()
	transferRegister(t, state, "remote", "remote-process", "active", 2)
	transferRegister(t, state, h.Cluster.Router.BrokerID(), "local-process", "active", 2)
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
	m.Replicas, m.ISR, m.Leader = []string{"remote"}, []string{"remote"}, "remote"
	m.LeaderEpoch++
	m.CommittedHWM = 5
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-1:"), data...)}))
	group := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("remote-scoped-%d", i)
		_, _, partition, _, err := h.Cluster.Router.FindCoordinatorWithEpoch(candidate)
		require.NoError(t, err)
		if partition == 0 {
			group = candidate
			break
		}
	}
	require.NotEmpty(t, group)
	require.NoError(t, h.Coordinator.RegisterGroupFromFSM("orders", group, 1))
	require.NoError(t, h.Coordinator.ApplyOffsetUpdateFromFSM(group, "orders", []coordinator.OffsetItem{{Partition: 0, Offset: 11}}))
	staged := &stagedConsumerTestReader{ends: []uint64{0, 5, 0, 0}, sources: map[int]ConsumerMetadataRecoverySource{1: {BrokerID: "remote"}}, records: map[int][]types.Message{1: stagedConsumerRecords(t, group)}}
	remote := state.GetBroker("remote")
	staged.sources[1] = ConsumerMetadataRecoverySource{BrokerID: remote.ID, IncarnationID: remote.IncarnationID, IncarnationEpoch: remote.IncarnationEpoch}
	staged.readHook = func() { assertConsumerReadUnderFSMFence(t, state) }
	h.SetConsumerMetadataRecoveryProvider(func(ctx context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
		require.Equal(t, []int{1}, request.Partitions)
		require.Zero(t, budget.Usage().Active, "install cannot reserve the provider's only work slot")
		releaseWork, err := budget.AcquireWork(ctx, "remote")
		require.NoError(t, err)
		releaseWork()
		release, err := h.Coordinator.BeginConsumerMetadataRecovery(ctx)
		require.NoError(t, err)
		release()
		transferRegister(t, state, "remote", "remote-process", "active", 2)
		return staged, nil
	})
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), identity, receiver, nil, nil, func(bool) error {
		require.Equal(t, 1, budget.Usage().Active, "publication must own an installation slot")
		return nil
	}))
	require.Zero(t, budget.Usage().Active)
	require.True(t, staged.closed)
	require.False(t, p.BootstrapPending())
	require.NoError(t, h.Coordinator.RecoveryReadinessError())
	require.Equal(t, 2, staged.reads, "both short remote pages must contribute")
	offset, found := h.Coordinator.GetOffset(group, "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(37), offset, "scoped replay must replace the Raft offset using the nonlocal partition")
}

func TestReplicaOffsetsInstallDoesNotHoldProviderWorkSlot(t *testing.T) {
	h, p, receiver, _ := consumerBootstrapFixture(t)
	limits, err := config.DefaultRecoveryConfig().Limits()
	require.NoError(t, err)
	budget, err := recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	h.SetRecoveryBudget(budget)
	state := h.Cluster.RaftManager.GetFSM()
	transferRegister(t, state, "remote", "remote-process", "active", 2)
	transferRegister(t, state, h.Cluster.Router.BrokerID(), "local-process", "active", 2)
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
	m.Replicas, m.ISR, m.Leader = []string{"remote"}, []string{"remote"}, "remote"
	m.LeaderEpoch++
	m.CommittedHWM = 1
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-1:"), data...)}))
	providerResult := errors.New("controlled provider exit")
	called := false
	h.SetConsumerMetadataRecoveryProvider(func(ctx context.Context, _ ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
		called = true
		// With one atomic slot, this acquisition used to depend on the outer
		// install returning, while install was waiting for this provider.
		require.Zero(t, budget.Usage().Active)
		release, err := budget.AcquireWork(ctx, "remote")
		if err != nil {
			return nil, err
		}
		release()
		return nil, providerResult
	})
	r := &replicaBootstrapRecovery{ch: h, state: state, brokerID: h.Cluster.Router.BrokerID()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = r.install(ctx, p, receiver.Manifest(), receiver, nil, nil)
	require.True(t, called)
	require.ErrorIs(t, err, providerResult)
	require.Zero(t, budget.Usage().Active)
	require.Zero(t, budget.Usage().Waiters)
}

// These fixtures have no transactional records. Production staging must retain
// raw records/markers and apply receiver-current decisions under this fence.
func stagedConsumerRecords(t *testing.T, group string) []types.Message {
	t.Helper()
	return []types.Message{
		consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: group, Topic: "orders", PartitionCount: 1, Epoch: 1, Timestamp: time.Unix(1, 0)}, 0),
		consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: group, Topic: "orders", Epoch: 1, Revision: 1, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 37}}, Timestamp: time.Unix(2, 0)}, 4),
	}
}

func assertConsumerReadUnderFSMFence(t *testing.T, state *fsm.BrokerFSM) {
	t.Helper()
	require.ErrorIs(t, state.WithConsumerMetadataRecoveryFence(func([]fsm.PartitionMetadata) error {
		t.Error("staged records were read outside final FSM exclusion")
		return nil
	}), fsm.ErrConsumerMetadataFencePending)
}

func TestUnknownOriginalStartupRejectsMissingCommittedCoverage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportStateWithRecovery(nil, nil))
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "remote")
	installRoutingOffsetsTopology(t, state, "remote")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.CommittedHWM = 5
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
	h := newCoordinatorRoutingHandler("unassigned", state, c)
	h.TopicManager = tm
	require.ErrorIs(t, h.RecoverDistributedConsumerMetadata(context.Background()), ErrConsumerRecoveryPending)
	require.Error(t, c.RecoveryReadinessError())
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
}

func TestNonReplicaConsumerRecoveryStagesWithoutLeaseOrFSMFence(t *testing.T) {
	for _, mutation := range []string{"none", "generation", "metadata", "source incarnation", "requester incarnation", "source capability", "requester capability"} {
		t.Run(mutation, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
			t.Cleanup(tm.Stop)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			state := fsm.NewBrokerFSM(tm, c)
			transferRegister(t, state, "remote", "remote-process", "active", 2)
			transferRegister(t, state, "unassigned", "local-process", "active", 2)
			installRoutingOffsetsTopology(t, state, "remote")
			m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
			m.CommittedHWM = 5
			data, err := json.Marshal(m)
			require.NoError(t, err)
			require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
			h := newCoordinatorRoutingHandler("unassigned", state, c)
			h.TopicManager = tm
			require.ErrorIs(t, h.RecoverDistributedConsumerMetadata(context.Background()), ErrConsumerRecoveryPending)
			staged := &stagedConsumerTestReader{ends: []uint64{5, 0, 0, 0}, records: map[int][]types.Message{0: stagedConsumerRecords(t, "remote-workers")}}
			remote := state.GetBroker("remote")
			staged.sources = map[int]ConsumerMetadataRecoverySource{0: {BrokerID: remote.ID, IncarnationID: remote.IncarnationID, IncarnationEpoch: remote.IncarnationEpoch}}
			staged.readHook = func() { assertConsumerReadUnderFSMFence(t, state) }
			called := false
			h.SetConsumerMetadataRecoveryProvider(func(ctx context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
				called = true
				require.Equal(t, []int{0}, request.Partitions)
				release, err := c.BeginConsumerMetadataRecovery(ctx)
				require.NoError(t, err)
				release()
				// A normal FSM transition must not wait for a fence held by staging.
				transferRegister(t, state, "remote", "remote-process", "active", 2)
				switch mutation {
				case "requester incarnation":
					transferRegister(t, state, "unassigned", "replacement", "active", 2)
				case "source capability":
					transferRegister(t, state, "remote", "remote-process", "active", 0)
				case "requester capability":
					transferRegister(t, state, "unassigned", "local-process", "active", 0)
				case "generation":
					generation := c.BeginConsumerRestore()
					c.FinishConsumerRestore(generation, nil)
				case "metadata":
					m.CommittedHWM++
					data, err := json.Marshal(m)
					require.NoError(t, err)
					require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
				case "source incarnation":
					broker := state.GetBroker("remote")
					broker.IncarnationID = "replacement"
					data, err := json.Marshal(broker)
					require.NoError(t, err)
					require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
				}
				return staged, nil
			})
			err = h.RecoverDistributedConsumerMetadata(context.Background())
			require.True(t, called)
			require.True(t, staged.closed)
			if mutation != "none" {
				require.ErrorIs(t, err, ErrConsumerRecoveryPending)
				require.Error(t, c.RecoveryReadinessError())
			} else {
				require.NoError(t, err)
				require.NoError(t, c.RecoveryReadinessError())
				require.Empty(t, h.groupRecoveryEpoch, "this broker owns no metadata partition")
				require.Equal(t, 2, staged.reads)
				offset, found := c.GetOffset("remote-workers", "orders", 0)
				require.True(t, found)
				require.Equal(t, uint64(37), offset)
			}
		})
	}
}
