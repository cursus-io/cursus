package fsm

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaRequestSelectsMarkedHistoryBeforeTailTrim(t *testing.T) {
	f, p := indexedRecoveryFSM(t, nil)
	retainedRecoveryCapabilities(f)
	// Current policy is delete; durable marks still authorize historical holes,
	// including the absent previous record at offset seven.
	require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 2, Payload: "kept"}}, 8))
	require.NoError(t, p.ApplyReplicaHWM(3))
	f.partitionMetadata["orders-0"].CommittedHWM = 3
	r, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, BootstrapRecoveryProtocolVersion, r.RecoveryProtocol)
	require.Equal(t, uint64(8), r.NextOffset)
	require.Equal(t, uint64(8), p.NextOffset(), "selection must preserve the original speculative tail")
	require.Empty(t, r.PreviousRecordDigest)
	require.Empty(t, r.PrefixCandidates)
	require.Equal(t, uint64(8), r.RetainedPrefix.End)
	require.NoError(t, p.ValidateReplicaRequestWitness(context.Background(), r.LocalWitness, 8))
	batch, err := f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	require.ErrorIs(t, ValidateReplicaRecoveryMode(batch), ErrReplicaBootstrapRequired)
	require.Nil(t, batch.TruncateTo)
}

func TestReplicaRequestOrdinaryCapablePeersKeepIndexedRecovery(t *testing.T) {
	f, p := indexedRecoveryFSM(t, nil)
	retainedRecoveryCapabilities(f)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}, {Offset: 1, Payload: "speculative"}}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	f.partitionMetadata["orders-0"].CommittedHWM = 1
	r, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, IndexedRecoveryProtocolVersion, r.RecoveryProtocol)
	require.Equal(t, uint64(1), p.NextOffset())
	require.NotEmpty(t, r.PreviousRecordDigest)
	require.Nil(t, r.RetainedPrefix)
	require.NoError(t, p.ValidateReplicaRequestWitness(context.Background(), r.LocalWitness, 1))
}

func TestReplicaRequestRejectsMetadataChangesDuringRetainedScan(t *testing.T) {
	for _, change := range []string{"leader", "incarnation", "capability", "definition", "closed storage", "cancel"} {
		t.Run(change, func(t *testing.T) {
			var provider *retainedSourceTestProvider
			f, p := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &retainedSourceTestProvider{DiskManager: dm}
				return provider
			})
			retainedRecoveryCapabilities(f)
			require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 1, Payload: "kept"}}, 3))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			provider.storage.afterPrefix = func() {
				// This lock must remain available while the proof scans disk.
				f.transitionMu.Lock()
				defer f.transitionMu.Unlock()
				switch change {
				case "leader":
					f.partitionMetadata["orders-0"].LeaderEpoch++
				case "incarnation":
					f.brokers["node-1"].IncarnationID = "replacement"
				case "capability":
					f.brokers["node-1"].RecoveryProtocol = 0
				case "definition":
					f.topicState["orders"].Revision++
				case "closed storage":
					require.NoError(t, provider.storage.Close())
				case "cancel":
					cancel()
				}
			}
			r, ok, err := f.BuildReplicaCatchupRequestContext(ctx, "node-2", "orders", 0)
			require.Error(t, err)
			require.False(t, ok)
			require.Nil(t, r.LocalWitness)
			require.Equal(t, uint64(3), p.NextOffset(), "builder cannot trim on a stale proof")
		})
	}
}

func TestReplicaRequestCancelsQueuedMetadataFence(t *testing.T) {
	f, _ := indexedRecoveryFSM(t, nil)
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, ok, err := f.BuildReplicaCatchupRequestContext(ctx, "node-2", "orders", 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, ok)
}

func TestReplicaRequestCanceledTailRepairReleasesFSMWithoutMutation(t *testing.T) {
	f, p := indexedRecoveryFSM(t, nil)
	retainedRecoveryCapabilities(f)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "committed"}, {Offset: 1, Payload: "speculative"}}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	f.partitionMetadata["orders-0"].CommittedHWM = 1
	release := p.BeginReplicationMutation()
	defer func() {
		if release != nil {
			release()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := f.BuildReplicaCatchupRequestContext(ctx, "node-2", "orders", 0); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		release()
		release = nil
		<-done
		t.Fatal("canceled builder retained FSM gates while waiting for tail repair")
	}
	// The local partition remains busy, but unrelated metadata must progress.
	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	unlock, err := lockSourceContext(gateCtx, &f.transitionMu, &f.materializationMu)
	require.NoError(t, err)
	unlock()
	release()
	release = nil
	require.Equal(t, uint64(2), p.NextOffset(), "canceled repair cannot truncate after the owner releases its lock")
	require.Equal(t, uint64(1), p.GetHWM())
	require.NoError(t, p.RecoveryError())
	_, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ok, "a fresh turn can still perform the repair")
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestReplicaRequestAllowsCommittedAdvanceDuringProof(t *testing.T) {
	var provider *retainedSourceTestProvider
	f, p := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &retainedSourceTestProvider{DiskManager: dm}
		return provider
	})
	retainedRecoveryCapabilities(f)
	require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 1, Payload: "kept"}}, 3))
	provider.storage.afterPrefix = func() {
		f.transitionMu.Lock()
		defer f.transitionMu.Unlock()
		f.mu.Lock()
		f.partitionMetadata["orders-0"].CommittedHWM = 2
		f.mu.Unlock()
	}
	r, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ok, "ordinary commits during proof work must not starve recovery")
	require.Zero(t, r.CommittedHWM, "keep the original request boundary")
	require.Equal(t, uint64(3), p.NextOffset())
	require.NotNil(t, r.LocalWitness)
}

type advancingLineageProvider struct {
	*disk.DiskManager
	after func()
}
type advancingLineageStorage struct {
	*disk.DiskHandler
	owner *advancingLineageProvider
}

func (p *advancingLineageProvider) GetHandlerWithPolicy(name string, id int, cleanup string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, cleanup, hours, bytes)
	if err != nil {
		return nil, err
	}
	return &advancingLineageStorage{h.(*disk.DiskHandler), p}, nil
}
func (s *advancingLineageStorage) LineageCandidatesContext(ctx context.Context, tail uint64) ([]disk.LineagePrefix, error) {
	proofs, err := s.DiskHandler.LineageCandidatesContext(ctx, tail)
	if s.owner.after != nil {
		s.owner.after()
	}
	return proofs, err
}

func TestReplicaRequestStillRejectsAdvanceBeforeDestructiveTrim(t *testing.T) {
	var provider *advancingLineageProvider
	f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "a"}, {Offset: 1, Payload: "b"}, {Offset: 2, Payload: "c"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &advancingLineageProvider{DiskManager: dm}
		return provider
	})
	retainedRecoveryCapabilities(f)
	f.partitionMetadata["orders-0"].CommittedHWM = 1
	provider.after = func() {
		f.transitionMu.Lock()
		defer f.transitionMu.Unlock()
		f.mu.Lock()
		f.partitionMetadata["orders-0"].CommittedHWM = 2
		f.mu.Unlock()
	}
	_, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.ErrorContains(t, err, "metadata changed")
	require.False(t, ok)
	require.Equal(t, uint64(3), p.NextOffset(), "never trim to an obsolete committed boundary")
}

func TestReplicaRequestDiscoveryAndMissingTargetDoNotMutateTail(t *testing.T) {
	f, p := indexedRecoveryFSM(t, nil)
	require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 1, Payload: "kept"}}, 3))
	keys := f.ReplicaCatchupCandidates("node-2")
	require.Equal(t, []ReplicaCatchupCandidate{{Topic: "orders", Partition: 0}}, keys)
	_, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "absent", 0)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, f.BuildReplicaCatchupRequestsExcluding("node-2", func(string, int) bool { return true }))
	require.Equal(t, uint64(3), p.NextOffset())
}
