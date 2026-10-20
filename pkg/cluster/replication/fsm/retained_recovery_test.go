package fsm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func retainedRecoveryCapabilities(f *BrokerFSM) {
	for _, broker := range f.brokers {
		broker.RecoveryProtocol = BootstrapRecoveryProtocolVersion
		broker.IncarnationID, broker.IncarnationEpoch = broker.ID+"-process", 1
	}
}

func retainedRequest(t *testing.T, f *BrokerFSM, p *topic.Partition, end uint64) ReplicaCatchupRequest {
	t.Helper()
	proof, err := p.ReplicaRetainedPrefixContext(context.Background(), 0, end)
	require.NoError(t, err)
	m := f.partitionMetadata["orders-0"]
	return ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", SourceBroker: "node-1", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch,
		LifecycleEpoch: m.LifecycleEpoch, NextOffset: end, CommittedHWM: m.CommittedHWM, MaxRecords: 1,
		RecoveryProtocol: BootstrapRecoveryProtocolVersion, RetainedPrefix: &proof.Proof}
}

func TestRetainedRecoverySourceMarkedRangesAndBootstrap(t *testing.T) {
	f, p := indexedRecoveryFSM(t, nil)
	retainedRecoveryCapabilities(f)
	// Background data flushes can temporarily hold the publication gate. Retry
	// only that documented contention result; all proof failures remain fatal.
	fetch := func(r ReplicaCatchupRequest) ReplicaCatchupBatch {
		t.Helper()
		var batch ReplicaCatchupBatch
		var err error
		require.Eventually(t, func() bool {
			batch, err = f.FetchReplicaCatchup(r)
			return !errors.Is(err, disk.ErrContentPublicationBusy)
		}, 5*time.Second, time.Millisecond)
		require.NoError(t, err)
		return batch
	}
	require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 2, Payload: "first"}, {Offset: 5, Payload: "last"}}, 8))
	require.NoError(t, p.ApplyReplicaHWM(8))
	f.partitionMetadata["orders-0"].CommittedHWM = 8
	for _, end := range []uint64{0, 1, 3, 6, 8} {
		r := retainedRequest(t, f, p, end)
		batch := fetch(r)
		require.NoError(t, ValidateReplicaRecoveryMode(batch))
		require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
		require.Equal(t, r.RetainedPrefix, batch.RetainedPrefix)
		require.Equal(t, r.RetainedPrefix, batch.MatchedRetainedPrefix)
		require.Nil(t, batch.TruncateTo)
		require.False(t, batch.BootstrapRequired)
		if end < 8 {
			require.True(t, batch.Compacted)
		}
	}
	for _, mismatch := range []string{"digest", "anchor", "beyond cut"} {
		r := retainedRequest(t, f, p, 3)
		switch mismatch {
		case "digest":
			r.RetainedPrefix.Digest = strings.Repeat("a", 64)
		case "anchor":
			r.RetainedPrefix.Start = 1
		case "beyond cut":
			r.NextOffset, r.RetainedPrefix.End = 9, 9
		}
		batch := fetch(r)
		require.ErrorIs(t, ValidateReplicaRecoveryMode(batch), ErrReplicaBootstrapRequired)
		require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
		require.Equal(t, r.RetainedPrefix, batch.RetainedPrefix)
		require.Nil(t, batch.MatchedRetainedPrefix)
		require.Nil(t, batch.TruncateTo)
		require.Empty(t, batch.Messages)
		require.False(t, batch.Verified)
	}
}

type retainedSourceTestStorage struct {
	*disk.DiskHandler
	afterPrefix func()
	prefixErr   error
}

func (s *retainedSourceTestStorage) RetainedPrefixContext(ctx context.Context, start, end uint64) (disk.RetainedPrefixResult, error) {
	if s.prefixErr != nil {
		return disk.RetainedPrefixResult{}, s.prefixErr
	}
	proof, err := s.DiskHandler.RetainedPrefixContext(ctx, start, end)
	if s.afterPrefix != nil {
		s.afterPrefix()
	}
	return proof, err
}

type retainedSourceTestProvider struct {
	*disk.DiskManager
	storage *retainedSourceTestStorage
}

func (p *retainedSourceTestProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	p.storage = &retainedSourceTestStorage{DiskHandler: h.(*disk.DiskHandler)}
	return p.storage, nil
}

func TestRetainedRecoveryRechecksOriginalSourceAfterUnlockedScan(t *testing.T) {
	for _, mutation := range []string{"none", "leader", "source process", "receiver process", "capability", "inactive", "ISR", "HWM advance", "HWM regression", "generation", "cancel"} {
		t.Run(mutation, func(t *testing.T) {
			var provider *retainedSourceTestProvider
			f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &retainedSourceTestProvider{DiskManager: dm}
				return provider
			})
			retainedRecoveryCapabilities(f)
			r := retainedRequest(t, f, p, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			provider.storage.afterPrefix = func() {
				// A prefix scan must not retain either global transition gate.
				require.True(t, f.transitionMu.TryLock())
				defer f.transitionMu.Unlock()
				require.True(t, f.materializationMu.TryLock())
				defer f.materializationMu.Unlock()
				f.mu.Lock()
				defer f.mu.Unlock()
				switch mutation {
				case "leader":
					f.partitionMetadata["orders-0"].LeaderEpoch++
				case "source process":
					f.brokers["node-1"].IncarnationEpoch++
				case "receiver process":
					f.brokers["node-2"].IncarnationID = "new"
				case "capability":
					f.brokers["node-1"].RecoveryProtocol = 1
				case "inactive":
					f.brokers["node-1"].Status = "inactive"
				case "ISR":
					f.partitionMetadata["orders-0"].ISR = append(f.partitionMetadata["orders-0"].ISR, "node-2")
				case "HWM advance":
					f.partitionMetadata["orders-0"].CommittedHWM++
				case "HWM regression":
					f.partitionMetadata["orders-0"].CommittedHWM--
				case "generation":
					require.NoError(t, provider.storage.TruncateTo(0))
				case "cancel":
					cancel()
				}
			}
			batch, err := f.FetchReplicaCatchupContext(ctx, r)
			if mutation == "none" || mutation == "HWM advance" {
				require.NoError(t, err)
				require.Equal(t, uint64(2), batch.CommittedHWM, "keep the original fixed cut")
			} else {
				require.Error(t, err)
				require.Empty(t, batch.Digest)
			}
		})
	}
}

func TestRetainedRecoverySourceOnlyMarkedFallbackRejectsCorruption(t *testing.T) {
	for _, damage := range []string{"none", "unmarked gap", "I/O"} {
		var provider *retainedSourceTestProvider
		f, p := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
			provider = &retainedSourceTestProvider{DiskManager: dm}
			return provider
		})
		retainedRecoveryCapabilities(f)
		proofs, err := p.ReplicaLineageCandidates(0)
		require.NoError(t, err)
		require.NoError(t, p.ReplicaAppendCompactedRange(nil, 4))
		require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 4, Payload: "tail"}}, true))
		require.NoError(t, p.ApplyReplicaHWM(5))
		f.partitionMetadata["orders-0"].CommittedHWM = 5
		r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4,
			LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: 5, MaxRecords: 1, RecoveryProtocol: 1, PrefixCandidates: proofs}
		injected := errors.New("prefix I/O failure")
		switch damage {
		case "unmarked gap":
			require.NoError(t, os.Truncate(provider.storage.GetSegmentPath(4), 0))
		case "I/O":
			provider.storage.prefixErr = injected
		}
		batch, err := f.FetchReplicaCatchup(r)
		if damage == "none" {
			require.NoError(t, err)
			require.ErrorIs(t, ValidateReplicaRecoveryMode(batch), ErrReplicaBootstrapRequired)
			require.Nil(t, batch.MatchedPrefix)
			require.Nil(t, batch.TruncateTo)
			require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
		} else {
			require.Error(t, err)
			require.False(t, batch.BootstrapRequired)
			if damage == "I/O" {
				require.ErrorIs(t, err, injected)
			}
		}
	}
}

func TestRetainedRecoveryJSONRequiresExplicitZeroBounds(t *testing.T) {
	valid := fmt.Sprintf(`{"recovery_protocol":2,"retained_prefix":{"version":1,"start":0,"end":0,"digest":%q}}`, strings.Repeat("a", 64))
	var request ReplicaCatchupRequest
	require.NoError(t, json.Unmarshal([]byte(valid), &request))
	for _, field := range []string{`"version":1,`, `"start":0,`, `"end":0,`} {
		raw := strings.Replace(valid, field, "", 1)
		require.Error(t, json.Unmarshal([]byte(raw), &request), field)
		var batch ReplicaCatchupBatch
		require.Error(t, json.Unmarshal([]byte(raw), &batch), field)
	}
	for _, raw := range []string{`{"recovery_protocol":2}`, `{"recovery_protocol":2,"retained_prefix":null}`, `{"retained_prefix":null}`, strings.Replace(valid, `"recovery_protocol":2`, `"recovery_protocol":1`, 1)} {
		require.Error(t, json.Unmarshal([]byte(raw), &request))
	}
	for _, mode := range []int{0, 1} {
		var batch ReplicaCatchupBatch
		require.Error(t, json.Unmarshal([]byte(fmt.Sprintf(`{"recovery_protocol":%d,"matched_retained_prefix":null}`, mode)), &batch))
		require.Error(t, json.Unmarshal([]byte(fmt.Sprintf(`{"recovery_protocol":%d,"matched_retained_prefix":null}`, mode)), &request))
	}
	for _, name := range []string{"Retained_Prefix", "RETAINED_PREFIX"} {
		raw := strings.Replace(valid, "retained_prefix", name, 1)
		require.Error(t, json.Unmarshal([]byte(raw), &request))
		var batch ReplicaCatchupBatch
		require.Error(t, json.Unmarshal([]byte(raw), &batch))
	}
}

func TestRetainedRecoveryRejectsModesAndMutualCapability(t *testing.T) {
	for _, variant := range []string{"source old", "receiver old", "in ISR", "candidate", "previous", "end", "version", "digest", "legacy field", "indexed field"} {
		f, p := indexedRecoveryFSM(t, nil)
		retainedRecoveryCapabilities(f)
		r := retainedRequest(t, f, p, 0)
		switch variant {
		case "source old":
			f.brokers["node-1"].RecoveryProtocol = 1
		case "receiver old":
			f.brokers["node-2"].RecoveryProtocol = 1
		case "in ISR":
			f.partitionMetadata["orders-0"].ISR = append(f.partitionMetadata["orders-0"].ISR, "node-2")
		case "candidate":
			r.PrefixCandidates = []disk.LineagePrefix{{}}
		case "previous":
			r.PreviousRecordDigest = "legacy"
		case "end":
			r.RetainedPrefix.End = 1
		case "version":
			r.RetainedPrefix.Version = 2
		case "digest":
			r.RetainedPrefix.Digest = strings.Repeat("g", 64)
		case "legacy field":
			r.RecoveryProtocol = 0
		case "indexed field":
			r.RecoveryProtocol = 1
		}
		_, err := f.FetchReplicaCatchup(r)
		require.Error(t, err, variant)
	}
}

func TestRetainedRecoveryContextAndWarmProofWork(t *testing.T) {
	var provider *retainedSourceTestProvider
	f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &retainedSourceTestProvider{DiskManager: dm}
		return provider
	})
	retainedRecoveryCapabilities(f)
	r := retainedRequest(t, f, p, 1)
	before := provider.storage.RetainedPrefixWorkSnapshot()
	for i := 0; i < 3; i++ {
		_, err := f.FetchReplicaCatchup(r)
		require.NoError(t, err)
	}
	after := provider.storage.RetainedPrefixWorkSnapshot()
	require.Equal(t, before.BytesRead, after.BytesRead)
	require.Equal(t, before.CanonicalBytes, after.CanonicalBytes)
	for _, materialization := range []bool{false, true} {
		if materialization {
			f.materializationMu.Lock()
		} else {
			f.transitionMu.Lock()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := f.FetchReplicaCatchupContext(ctx, r)
		cancel()
		if materialization {
			f.materializationMu.Unlock()
		} else {
			f.transitionMu.Unlock()
		}
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	// An old third broker does not disable pairwise replica recovery.
	f.brokers["third"] = &BrokerInfo{ID: "third", Status: "active", RecoveryProtocol: 0}
	_, err := f.FetchReplicaCatchup(r)
	require.NoError(t, err)
}
