package fsm

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestFinalizeRecoveredPartitionsPreservesBootstrapPending(t *testing.T) {
	manager, partition := newDurableFSMTopic(t, "orders")
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	source, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()
	require.NoError(t, source.WriteCompactedReplicaRange(0, 4, nil))
	fence := disk.BootstrapFence{Topic: "orders", Partition: 0, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 1, LifecycleEpoch: manager.GetTopic("orders").LifecycleEpoch, CommittedHWM: 4, RecoveryProtocol: 3}
	export, err := source.ExportBootstrap(0, 4, fence, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, export.Close()) }()
	receiver, err := partition.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	for _, f := range export.Manifest().Files {
		if f.Size > 0 {
			chunk, err := export.ReadChunk(f.ID, 0, disk.BootstrapChunkBytes)
			require.NoError(t, err)
			require.NoError(t, receiver.WriteChunk(f.ID, 0, chunk))
		}
	}
	session, err := partition.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, session.Install(receiver, nil))
	session.Close()
	f := NewBrokerFSM(manager, nil)
	f.partitionMetadata["orders-0"] = &PartitionMetadata{CommittedHWM: 4, CommittedHWMKnown: false}
	f.BeginRecoveredPartitionReplay()
	require.NoError(t, f.FinalizeRecoveredPartitions())
	require.False(t, f.HasPendingPartitionRecovery())
	require.True(t, partition.BootstrapPending())
	require.False(t, partition.HWMKnown())
	metadata, err := partition.BootstrapStatus()
	require.NoError(t, err)
	require.Equal(t, export.Manifest().TransferID, metadata.TransferID)
}
