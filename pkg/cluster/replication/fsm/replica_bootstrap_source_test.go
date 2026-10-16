package fsm

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func replicaBootstrapFSM(t *testing.T) (*BrokerFSM, *topic.Partition, *disk.DiskHandler, BootstrapBeginRequest) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 1, .1
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { tm.Stop(); dm.CloseAllHandlers() })
	def := topic.DefaultDefinition("orders", cfg)
	def.ReplicationFactor = 2
	def.Policy.ReadACL = []string{"reader"}
	def.Policy.WriteACL = []string{"writer"}
	minimum := 1
	def.Policy.MinInSyncReplicas = &minimum
	require.NoError(t, tm.ApplyDefinition(def))
	p, err := tm.GetTopic(def.Name).GetPartition(0)
	require.NoError(t, err)
	r := metadataBeginRequest()
	r.Purpose, r.Topic, r.MinimumEnd = BootstrapPurposeReplica, def.Name, 1
	f := NewBrokerFSM(tm, nil)
	def = tm.GetTopic(def.Name).Definition()
	f.topicState[def.Name] = &def
	f.partitionMetadata[def.Name+"-0"] = &PartitionMetadata{Leader: r.Leader, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, PartitionCount: def.Partitions, CommittedHWMKnown: true, CommittedHWM: 1, Replicas: []string{r.Source.BrokerID, r.Requester.BrokerID}, ISR: []string{r.Source.BrokerID}}
	for _, id := range []BootstrapProcessIdentity{r.Source, r.Requester} {
		f.brokers[id.BrokerID] = &BrokerInfo{ID: id.BrokerID, Status: "active", IncarnationID: id.IncarnationID, IncarnationEpoch: id.IncarnationEpoch, RecoveryProtocol: 2}
	}
	// An unrelated older voter must not disable mutual source/requester support.
	f.brokers["older"] = &BrokerInfo{ID: "older", Status: "active", RecoveryProtocol: 0}
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Key: "a", Payload: "zero"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(1))
	h, err := dm.GetHandler(r.Topic, 0)
	require.NoError(t, err)
	return f, p, h.(*disk.DiskHandler), r
}

func replicaCapture(t *testing.T, ticket *ReplicaBootstrapSourceTicket) *ReplicaBootstrapCapture {
	t.Helper()
	capture, err := ticket.CaptureContext(context.Background(), func(source *topic.BootstrapSource, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
		return source.ExportOwnedContext(context.Background(), fence, []byte("catalog"))
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	return capture
}

func TestReplicaBootstrapSelectsActualEndOnceAndAllowsGrowth(t *testing.T) {
	f, p, _, r := replicaBootstrapFSM(t)
	ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 1, Payload: "one"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(2))
	f.partitionMetadata[r.Topic+"-0"].CommittedHWM = 2
	capture := replicaCapture(t, ticket)
	require.EqualValues(t, 2, capture.Manifest().EndOffset)
	require.EqualValues(t, 2, capture.Manifest().Fence.CommittedHWM)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 2, Payload: "two"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(3))
	f.partitionMetadata[r.Topic+"-0"].CommittedHWM = 3
	var escaped *ReplicaBootstrapPublication
	require.NoError(t, f.WithReplicaBootstrapSourceFence(context.Background(), capture, func(scope *ReplicaBootstrapPublication) error {
		escaped = scope
		return scope.TryPublishOriginal(func() {})
	}))
	require.EqualValues(t, 2, capture.Manifest().EndOffset)
	require.ErrorIs(t, escaped.TryPublishOriginal(func() { t.Fatal("expired publication") }), ErrBootstrapTransferStaleFence)
}

func TestReplicaBootstrapRejectsChangedAuthority(t *testing.T) {
	for _, change := range []string{"requester ISR", "requester unassigned", "source nonISR", "source identity", "requester identity", "source capability", "requester capability", "leader", "lifecycle", "unknown HWM", "low HWM", "local HWM", "partition owner", "read ACL", "write ACL", "minimum ISR", "revision", "generation", "retention", "recovery pending"} {
		t.Run(change, func(t *testing.T) {
			f, p, d, r := replicaBootstrapFSM(t)
			ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
			require.NoError(t, err)
			capture := replicaCapture(t, ticket)
			m, def := f.partitionMetadata[r.Topic+"-0"], f.topicState[r.Topic]
			switch change {
			case "requester ISR":
				m.ISR = append(m.ISR, r.Requester.BrokerID)
			case "requester unassigned":
				m.Replicas = m.Replicas[:1]
			case "source nonISR":
				m.ISR = nil
			case "source identity":
				f.brokers[r.Source.BrokerID].IncarnationEpoch++
			case "requester identity":
				f.brokers[r.Requester.BrokerID].IncarnationEpoch++
			case "source capability":
				f.brokers[r.Source.BrokerID].RecoveryProtocol = 0
			case "requester capability":
				f.brokers[r.Requester.BrokerID].RecoveryProtocol = 0
			case "leader":
				m.LeaderEpoch++
			case "lifecycle":
				def.LifecycleEpoch++
			case "unknown HWM":
				m.CommittedHWMKnown = false
			case "low HWM":
				m.CommittedHWM = 0
			case "local HWM":
				p.HWM = 0
			case "recovery pending":
				p.MarkReconciliationPending(errors.New("repair required"))
			case "partition owner":
				f.tm.GetTopic(r.Topic).Partitions[0] = nil
				defer func() { f.tm.GetTopic(r.Topic).Partitions[0] = p }()
			case "read ACL":
				def.Policy.ReadACL[0] = "changed"
			case "write ACL":
				def.Policy.WriteACL[0] = "changed"
			case "minimum ISR":
				*def.Policy.MinInSyncReplicas = 2
			case "revision":
				def.Revision++
			case "generation":
				require.NoError(t, d.TruncateTo(0))
			case "retention":
				require.NoError(t, d.RollSegmentAt(1))
				d.SetRetentionPolicy(0, 1)
				d.EnforceRetention(&config.Config{})
				anchor, err := d.GetFirstOffsetContext(context.Background())
				require.NoError(t, err)
				require.EqualValues(t, 1, anchor)
			}
			err = f.WithReplicaBootstrapSourceFence(context.Background(), capture, func(scope *ReplicaBootstrapPublication) error {
				return scope.TryPublishOriginal(func() { t.Fatal("stale capture published") })
			})
			require.Error(t, err)
			// The ticket must not alias mutable authoritative policy memory.
			require.Equal(t, "reader", ticket.definition.Policy.ReadACL[0])
			require.Equal(t, "writer", ticket.definition.Policy.WriteACL[0])
			require.Equal(t, 1, *ticket.definition.Policy.MinInSyncReplicas)
		})
	}
}

func TestReplicaBootstrapOriginalGenerationInsidePublication(t *testing.T) {
	f, _, d, r := replicaBootstrapFSM(t)
	ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.NoError(t, err)
	capture := replicaCapture(t, ticket)
	err = f.WithReplicaBootstrapSourceFence(context.Background(), capture, func(scope *ReplicaBootstrapPublication) error {
		require.NoError(t, d.TruncateTo(0))
		return scope.TryPublishOriginal(func() { t.Fatal("original generation was replaced") })
	})
	require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
	other := NewBrokerFSM(nil, nil)
	require.ErrorIs(t, other.WithReplicaBootstrapSourceFence(context.Background(), capture, func(*ReplicaBootstrapPublication) error { t.Fatal("foreign capture"); return nil }), ErrBootstrapTransferStaleFence)
}

func TestReplicaBootstrapEmptyCompactedCaptureOutsideFSM(t *testing.T) {
	f, p, d, r := replicaBootstrapFSM(t)
	// Replace the retained log by an explicitly empty compacted logical extent.
	require.NoError(t, d.TruncateTo(0))
	require.NoError(t, d.WriteCompactedReplicaRange(0, 1, nil))
	ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.NoError(t, err)
	capture, err := ticket.CaptureContext(context.Background(), func(source *topic.BootstrapSource, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
		require.True(t, f.transitionMu.TryLock(), "full export held FSM transition")
		f.transitionMu.Unlock()
		require.True(t, f.materializationMu.TryLock(), "full export held materialization")
		f.materializationMu.Unlock()
		return source.ExportOwnedContext(context.Background(), fence, nil)
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, capture.Close()) }()
	require.EqualValues(t, 1, p.GetHWM())
	require.EqualValues(t, 1, capture.Manifest().EndOffset)
	require.Zero(t, capture.Manifest().Files[0].Size)
	require.True(t, capture.Manifest().Compacted)
	require.NoError(t, f.WithReplicaBootstrapSourceFence(context.Background(), capture, func(scope *ReplicaBootstrapPublication) error { return scope.TryPublishOriginal(func() {}) }))
}

func TestReplicaBootstrapCaptureCleansLateAndFailedExports(t *testing.T) {
	for _, failure := range []string{"callback", "outer canceled", "wrong manifest"} {
		t.Run(failure, func(t *testing.T) {
			f, _, _, r := replicaBootstrapFSM(t)
			ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var exported *disk.BootstrapExport
			capture, err := ticket.CaptureContext(ctx, func(source *topic.BootstrapSource, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
				if failure == "wrong manifest" {
					fence.SourceID = "another"
				}
				var exportErr error
				exported, exportErr = source.ExportOwnedContext(ctx, fence, nil)
				require.NoError(t, exportErr)
				if failure == "callback" {
					return exported, errors.New("catalog guard changed")
				}
				if failure == "outer canceled" {
					cancel()
				}
				return exported, nil
			})
			require.Error(t, err)
			require.Nil(t, capture)
			_, err = exported.ReadChunk("records", 0, 1)
			require.Error(t, err, "unpublished export remained live")
		})
	}
}

func TestReplicaBootstrapRejectsUnsupportedStorageAndUnknownTopic(t *testing.T) {
	f, _, _, r := metadataSourceFSM(t)
	r.Purpose, r.MinimumEnd, r.FixedEnd = BootstrapPurposeReplica, r.FixedEnd, 0
	f.partitionMetadata[r.Topic+"-0"].Replicas = append(f.partitionMetadata[r.Topic+"-0"].Replicas, r.Requester.BrokerID)
	ticket, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.NoError(t, err)
	_, err = ticket.CaptureContext(context.Background(), func(*topic.BootstrapSource, disk.BootstrapFence) (*disk.BootstrapExport, error) {
		t.Fatal("unsupported storage entered capture")
		return nil, nil
	})
	require.ErrorIs(t, err, ErrBootstrapTransferUnsupported)
	r.Topic = "unknown"
	_, err = f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
	r.Topic = "../unsafe"
	_, err = f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.Error(t, err)
}
