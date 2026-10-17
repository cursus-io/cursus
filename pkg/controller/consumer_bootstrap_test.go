package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func consumerBootstrapFixture(t *testing.T) (*CommandHandler, *topic.Partition, *disk.BootstrapReceiver, coordinator.ConsumerBootstrapIdentity) {
	h, p, r, id, _ := consumerBootstrapFixtureWithStorage(t)
	return h, p, r, id
}

func consumerBootstrapFixtureWithStorage(t *testing.T) (*CommandHandler, *topic.Partition, *disk.BootstrapReceiver, coordinator.ConsumerBootstrapIdentity, *disk.DiskHandler) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 1 << 20
	cfg.IndexSize = 4096
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, &dummyPublisher{})
	require.NoError(t, err)
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "local")
	installRoutingOffsetsTopology(t, state, "local")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager, h.Config = tm, cfg
	t.Cleanup(func() { _ = h.Close(); c.Stop(); tm.Stop(); dm.CloseAllHandlers() })
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	sourceCfg := *cfg
	sourceCfg.LogDir = t.TempDir()
	source, err := disk.NewDiskHandler(&sourceCfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	lifecycle := tm.GetTopic(config.ConsumerOffsetsTopicName).Definition().LifecycleEpoch
	export, err := source.ExportBootstrap(0, 0, disk.BootstrapFence{Topic: config.ConsumerOffsetsTopicName, Partition: 0, LifecycleEpoch: lifecycle, LeaderEpoch: 1, LeaderID: "local", SourceID: "local", ReceiverID: "local", RecoveryProtocol: 3}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, export.Close()) })
	receiver, err := p.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	for _, file := range export.Manifest().Files {
		for offset := int64(0); offset < file.Size; {
			chunk, err := export.ReadChunk(file.ID, offset, disk.BootstrapChunkBytes)
			require.NoError(t, err)
			require.NoError(t, receiver.WriteChunk(file.ID, offset, chunk))
			offset += int64(len(chunk))
		}
	}
	target, err := dm.GetHandler(config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	return h, p, receiver, coordinator.ConsumerBootstrapIdentity{Partition: 0, Lifecycle: lifecycle, Attempt: export.Manifest().TransferID}, target.(*disk.DiskHandler)
}

func TestConsumerBootstrapPreparedRetryAfterManifestRemovalAndRestore(t *testing.T) {
	h, p, receiver, id, d := consumerBootstrapFixtureWithStorage(t)
	c := h.Coordinator
	name := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("physical-%d", i)
		_, _, route, _, err := h.Cluster.Router.FindCoordinatorWithEpoch(candidate)
		require.NoError(t, err)
		if route != 0 {
			name = candidate
			break
		}
	}
	require.NotEmpty(t, name)
	require.NoError(t, c.RegisterGroupFromFSM("orders", name, 1))
	c.RecordConsumerMetadataSource(name, 0)
	paths, err := filepath.Glob(d.BaseName + ".bootstrap-*/" + receiver.Manifest().Files[1].ID)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	catalog := paths[0]
	require.Error(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, func(*topic.PartitionReadView, []byte) error { return os.Rename(catalog, catalog+".held") }))
	require.NoError(t, os.Rename(catalog+".held", catalog))
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.True(t, s.Prepared())
	s.Close()
	// Simulate the crash window after manifest removal but before the partition
	// owner observes completion. Retained prepared view remains the only reader.
	require.NoError(t, d.CompleteBootstrap(id.Attempt))
	generation := c.BeginConsumerRestore()
	proof := &coordinator.ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*coordinator.GroupStateSnapshot{}, SourcePartitions: map[string][]int{}}
	require.NoError(t, c.ImportStateWithRecovery(map[string]*coordinator.GroupStateSnapshot{name: {TopicName: "orders", Partitions: []int{0}, RegistrationEpoch: 1, LastActivity: time.Unix(1, 0)}}, proof))
	c.FinishConsumerRestore(generation, nil)
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), id, nil, nil, func(*topic.PartitionReadView, []byte) error {
		t.Fatal("prepared retry fetched removed catalog")
		return nil
	}))
	require.NotContains(t, c.ExportState(), name, "retained physical scope must survive import")
	require.False(t, p.BootstrapPending())
	require.True(t, c.ConsumerRecoveryRequirement().Pending(), "scoped success cannot complete full restore")
}

func TestConsumerBootstrapProtocolValidationRunsBeforeIntent(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	failure := errors.New("transfer fence changed")
	require.ErrorIs(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, nil, func(resuming bool) error {
		require.False(t, resuming)
		require.NotNil(t, h.Cluster.RaftManager.GetFSM().GetPartitionMetadata(config.ConsumerOffsetsTopicName+"-0"))
		return failure
	}), failure)
	require.False(t, p.BootstrapPending())
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
}

func TestConsumerBootstrapReconstructsManifestBarrierBeforeOpen(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Install(receiver, nil))
	s.Close()
	require.NoError(t, h.ReconstructConsumerBootstrapTickets())
	require.ErrorIs(t, h.Coordinator.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), id, nil, nil, nil))
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
}

func TestConsumerBootstrapPostIntentFailureRemainsClosedAndResumes(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	fail := errors.New("catalog unavailable")
	err := h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, func(*topic.PartitionReadView, []byte) error { return fail })
	require.ErrorIs(t, err, fail)
	require.True(t, p.BootstrapPending())
	require.ErrorIs(t, h.Coordinator.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), id, nil, nil, nil))
	require.False(t, p.BootstrapPending())
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
}

func TestDelayedBootstrapObservationCannotRecreateCompletedTicket(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	failure := errors.New("leave durable bootstrap pending")
	require.ErrorIs(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, func(*topic.PartitionReadView, []byte) error { return failure }), failure)
	// Pause an observer after its pending/status reads, before recording the
	// observed manifest. Completion runs fully inside this deterministic gap.
	observation := h.Coordinator.CaptureConsumerBootstrapObservation()
	require.True(t, p.BootstrapPending())
	manifest, _ := p.BootstrapStatus()
	require.NotNil(t, manifest)
	observed := coordinator.ConsumerBootstrapIdentity{Partition: manifest.Fence.Partition, Lifecycle: manifest.Fence.LifecycleEpoch, Attempt: manifest.TransferID}
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), id, nil, nil, nil))
	require.False(t, p.BootstrapPending())
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
	require.ErrorIs(t, h.Coordinator.ObservePendingConsumerBootstrap(observed, observation), coordinator.ErrConsumerBootstrapObservationStale)
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError(), "delayed observation must not resurrect a completed ticket")
	require.NoError(t, h.ReconstructConsumerBootstrapTickets())
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
}

func TestConsumerBootstrapPreIntentAbortReopensOnlyWithoutSplice(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	fail := errors.New("detach failed")
	require.ErrorIs(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, func() error { return fail }, nil), fail)
	require.False(t, p.BootstrapPending())
	require.NoError(t, h.Coordinator.ConsumerOperationsReadinessError())
}

func TestConsumerBootstrapRefusesSecondPendingPartitionBeforeInstall(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	other, err := h.TopicManager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(1)
	require.NoError(t, err)
	s, err := other.BeginBootstrapSession()
	require.NoError(t, err)
	defer s.Close()
	require.Error(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, nil))
	require.False(t, p.BootstrapPending(), "second disk transaction must not be installed")
}

func TestConsumerBootstrapUsesManifestEndWhenFSMHWMAdvanced(t *testing.T) {
	h, p, receiver, id := consumerBootstrapFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.CommittedHWM = 5
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
	require.NoError(t, h.RunConsumerMetadataBootstrap(context.Background(), id, receiver, nil, nil))
	require.False(t, p.BootstrapPending())
	require.Zero(t, p.GetHWM())
}
