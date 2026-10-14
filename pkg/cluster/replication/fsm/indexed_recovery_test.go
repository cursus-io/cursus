package fsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
	"time"
)

func TestIndexedRecoveryCapabilitySurvivesRegistrationAndSnapshot(t *testing.T) {
	state := NewBrokerFSM(nil, nil)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node-1","addr":"localhost:9001","status":"active","lifecycle_protocol":6,"recovery_protocol":99}`)}))
	require.Equal(t, 99, state.GetBroker("node-1").RecoveryProtocol)
	snapshot, err := state.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	buf := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buf}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.Equal(t, 99, restored.GetBroker("node-1").RecoveryProtocol)
	require.Equal(t, 6, restored.GetBroker("node-1").LifecycleProtocol)
}

func TestIndexedRecoveryRejectsUnsupportedOrStaleRequests(t *testing.T) {
	f, _ := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}})
	for _, broker := range f.brokers {
		broker.RecoveryProtocol = IndexedRecoveryProtocolVersion
	}
	request := f.BuildReplicaCatchupRequests("node-2")[0]
	for _, test := range []struct {
		name   string
		mutate func(*ReplicaCatchupRequest)
	}{
		{"unknown mandatory version", func(r *ReplicaCatchupRequest) { r.RecoveryProtocol = 99 }},
		{"legacy with indexed fields", func(r *ReplicaCatchupRequest) { r.RecoveryProtocol = 0 }},
		{"missing exact prefix", func(r *ReplicaCatchupRequest) { r.PrefixCandidates = r.PrefixCandidates[1:] }},
		{"leader epoch", func(r *ReplicaCatchupRequest) { r.LeaderEpoch++ }},
		{"lifecycle epoch", func(r *ReplicaCatchupRequest) { r.LifecycleEpoch++ }},
		{"receiver", func(r *ReplicaCatchupRequest) { r.BrokerID = "unknown" }},
		{"source", func(r *ReplicaCatchupRequest) { r.SourceBroker = "node-2" }},
		{"hwm", func(r *ReplicaCatchupRequest) { r.CommittedHWM++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := request
			test.mutate(&r)
			_, err := f.FetchReplicaCatchup(r)
			require.Error(t, err)
		})
	}
	f.partitionMetadata["orders-0"].ISR = append(f.partitionMetadata["orders-0"].ISR, "node-2")
	_, err := f.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "outside ISR")
}

func TestIndexedRecoveryNoComparablePrefixRequiresBootstrap(t *testing.T) {
	f, p := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}})
	for _, broker := range f.brokers {
		broker.RecoveryProtocol = IndexedRecoveryProtocolVersion
	}
	request := f.BuildReplicaCatchupRequests("node-2")[0]
	// A receiver retaining [1,2) cannot prove ancestry with the source's [0,2).
	request.PrefixCandidates = []disk.LineagePrefix{{StartOffset: 1, EndOffset: 2, Digest: strings.Repeat("a", 64)}, {StartOffset: 1, EndOffset: 1, Digest: strings.Repeat("b", 64)}}
	batch, err := f.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.True(t, batch.BootstrapRequired)
	require.Nil(t, batch.TruncateTo)
	require.Nil(t, batch.MatchedPrefix)
	require.ErrorIs(t, ValidateReplicaRecoveryMode(batch), ErrReplicaBootstrapRequired)
	require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
	require.Equal(t, uint64(2), p.NextOffset())
}

func TestIndexedRecoveryLegacyJSONChecksumRemainsCompatible(t *testing.T) {
	const old = `{"topic":"orders","partition":0,"broker_id":"node-2","start_offset":0,"end_offset":1,"committed_hwm":1,"leader":"node-1","source_broker":"node-1","leader_epoch":4,"lifecycle_epoch":1,"verified":true,"messages":null,"digest":""}`
	var batch ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal([]byte(old), &batch))
	sealed, err := SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(old))
	require.Equal(t, hex.EncodeToString(digest[:]), sealed.Digest)
	require.NoError(t, ValidateReplicaRecoveryMode(sealed))
}

func TestIndexedRecoveryFenceRejectsChangedMembershipAndCapability(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*BrokerFSM)
	}{
		{"receiver rejoined", func(f *BrokerFSM) {
			f.partitionMetadata["orders-0"].ISR = append(f.partitionMetadata["orders-0"].ISR, "node-2")
		}},
		{"inactive source", func(f *BrokerFSM) { f.brokers["node-1"].Status = "inactive" }},
		{"source downgraded", func(f *BrokerFSM) { f.brokers["node-1"].RecoveryProtocol = 0 }},
		{"leader changed", func(f *BrokerFSM) { f.partitionMetadata["orders-0"].LeaderEpoch++ }},
		{"lifecycle changed", func(f *BrokerFSM) { f.partitionMetadata["orders-0"].LifecycleEpoch++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _ := indexedRecoveryFSM(t, nil)
			for _, b := range f.brokers {
				b.RecoveryProtocol = IndexedRecoveryProtocolVersion
			}
			batch, err := f.FetchReplicaCatchup(f.BuildReplicaCatchupRequests("node-2")[0])
			require.NoError(t, err)
			test.mutate(f)
			called := false
			require.Error(t, f.WithReplicaRecoveryFence(batch, func() error { called = true; return nil }))
			require.False(t, called)
		})
	}
}

func indexedRecoveryFSM(t *testing.T, messages []types.Message) (*BrokerFSM, *topic.Partition) {
	return indexedRecoveryFSMWithProvider(t, messages, nil)
}

func indexedRecoveryFSMWithProvider(t *testing.T, messages []types.Message, wrap func(*disk.DiskManager) topic.HandlerProvider) (*BrokerFSM, *topic.Partition) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	var provider topic.HandlerProvider = dm
	if wrap != nil {
		provider = wrap(dm)
	}
	tm := topic.NewTopicManager(cfg, provider, nil)
	require.NoError(t, tm.CreateTopic("orders", 1, false, false))
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	if len(messages) > 0 {
		require.NoError(t, p.ReplicaAppendWithMode(messages, true))
	}
	require.NoError(t, p.ApplyReplicaHWM(uint64(len(messages))))
	f := NewBrokerFSM(tm, nil)
	registerActiveBroker(t, f, "node-1")
	registerActiveBroker(t, f, "node-2")
	definition := tm.GetTopic("orders").Definition()
	f.topicState["orders"] = &definition
	f.partitionMetadata["orders-0"] = &PartitionMetadata{Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch, CommittedHWM: uint64(len(messages)), CommittedHWMKnown: true, PartitionCount: 1, Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"}}
	return f, p
}

type blockingLineageProvider struct {
	*disk.DiskManager
	matched chan struct{}
	release chan struct{}
}

func (p *blockingLineageProvider) GetHandlerWithPolicy(name string, id int, cleanup string, hours int, bytes int64) (types.StorageHandler, error) {
	handler, err := p.DiskManager.GetHandlerWithPolicy(name, id, cleanup, hours, bytes)
	if err != nil {
		return nil, err
	}
	return &blockingLineageStorage{DiskHandler: handler.(*disk.DiskHandler), matched: p.matched, release: p.release}, nil
}

type blockingLineageStorage struct {
	*disk.DiskHandler
	matched chan struct{}
	release chan struct{}
}

func (s *blockingLineageStorage) MatchLineageCandidatesContext(ctx context.Context, candidates []disk.LineagePrefix, hwm uint64) (*disk.LineagePrefix, error) {
	matched, err := s.DiskHandler.MatchLineageCandidatesContext(ctx, candidates, hwm)
	close(s.matched)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return matched, err
}

func TestIndexedRecoverySourceKeepsProofAndRecordsInOneGeneration(t *testing.T) {
	matched, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	source, sourcePartition := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "generation-a"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		return &blockingLineageProvider{DiskManager: dm, matched: matched, release: release}
	})
	for _, broker := range source.brokers {
		broker.RecoveryProtocol = IndexedRecoveryProtocolVersion
		broker.LifecycleProtocol = BrokerProtocolVersionCurrent
	}
	request := source.BuildReplicaCatchupRequests("node-2")[0]
	// Empty-tail proofs have no previous-record digest, making mixed-generation
	// reads particularly dangerous: the cumulative proof must fence the read.
	request.NextOffset = 0
	request.PreviousRecordDigest = ""
	request.PreviousLeaderEpoch = 0
	var err error
	request.PrefixCandidates, err = sourcePartition.ReplicaLineageCandidates(0)
	require.NoError(t, err)
	type fetchResult struct {
		batch ReplicaCatchupBatch
		err   error
	}
	fetched := make(chan fetchResult, 1)
	go func() { batch, err := source.FetchReplicaCatchup(request); fetched <- fetchResult{batch, err} }()
	select {
	case <-matched:
	case <-time.After(3 * time.Second):
		t.Fatal("source never reached cumulative comparison")
	}
	truncated := make(chan interface{}, 1)
	go func() {
		truncated <- source.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`)})
	}()
	select {
	case result := <-truncated:
		t.Fatalf("truncate crossed the in-flight source generation: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	released = true
	select {
	case result := <-fetched:
		require.NoError(t, result.err)
		require.Len(t, result.batch.Messages, 1)
		require.Equal(t, "generation-a", result.batch.Messages[0].Payload)
		require.Equal(t, request.LifecycleEpoch, result.batch.LifecycleEpoch)
		require.NoError(t, ValidateReplicaCatchupBatchDigest(result.batch))
	case <-time.After(3 * time.Second):
		t.Fatal("source fetch did not complete")
	}
	select {
	case result := <-truncated:
		truncate, ok := result.(topic.TruncateResult)
		require.True(t, ok, "unexpected truncate result: %#v", result)
		require.True(t, truncate.Truncated)
		require.False(t, truncate.CleanupPending)
	case <-time.After(3 * time.Second):
		t.Fatal("truncate did not complete after fetch released its fence")
	}
	_, err = source.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "stale")
}

func TestIndexedRecoveryNegotiatesMutualCapability(t *testing.T) {
	for _, versions := range [][2]int{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {99, 99}} {
		f, _ := indexedRecoveryFSM(t, nil)
		f.brokers["node-1"].RecoveryProtocol = versions[0]
		f.brokers["node-2"].RecoveryProtocol = versions[1]
		requests := f.BuildReplicaCatchupRequests("node-2")
		require.Len(t, requests, 1)
		want := 0
		if versions[0] > 0 && versions[1] > 0 {
			want = IndexedRecoveryProtocolVersion
			require.NotEmpty(t, requests[0].PrefixCandidates)
		}
		require.Equal(t, want, requests[0].RecoveryProtocol)
	}
}

func TestIndexedRecoveryFindsFullPrefixAcrossLongDivergence(t *testing.T) {
	for _, test := range []struct {
		name            string
		count, firstBad int
		lastSame        bool
		want            uint64
	}{
		{"single bad last record", 1025, 1024, false, 1024},
		{"early divergence reconvergent last record", 64, 1, true, 1},
		{"long divergence", 1600, 100, true, 0},
	} {
		for _, epoch := range []int64{0, 7} {
			t.Run(fmt.Sprintf("%s/epoch-%d", test.name, epoch), func(t *testing.T) {
				sourceRecords := make([]types.Message, test.count)
				followerRecords := make([]types.Message, test.count)
				for i := range sourceRecords {
					sourceRecords[i] = types.Message{Offset: uint64(i), Payload: "source", LeaderEpoch: epoch}
					followerRecords[i] = sourceRecords[i]
					if i >= test.firstBad && (!test.lastSame || i != test.count-1) {
						followerRecords[i].Payload = "divergent"
					}
				}
				source, sp := indexedRecoveryFSM(t, sourceRecords)
				follower, fp := indexedRecoveryFSM(t, followerRecords)
				for _, f := range []*BrokerFSM{source, follower} {
					for _, b := range f.brokers {
						b.RecoveryProtocol = IndexedRecoveryProtocolVersion
					}
				}
				request := follower.BuildReplicaCatchupRequests("node-2")[0]
				require.LessOrEqual(t, len(request.PrefixCandidates), 64)
				batch, err := source.FetchReplicaCatchup(request)
				require.NoError(t, err)
				require.NotNil(t, batch.TruncateTo, "identical final records must not hide earlier divergence")
				require.Equal(t, test.want, *batch.TruncateTo)
				require.NotNil(t, batch.MatchedPrefix)
				require.Equal(t, uint64(test.count), sp.NextOffset(), "source must remain intact")
				require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
				require.NoError(t, fp.ApplyReplicaRollback(*batch.TruncateTo))
				exchanges := 1
				transferredRecords, transferredBytes := 0, 0
				for fp.NextOffset() < uint64(test.count) {
					request = follower.BuildReplicaCatchupRequests("node-2")[0]
					batch, err = source.FetchReplicaCatchup(request)
					require.NoError(t, err)
					require.Nil(t, batch.TruncateTo)
					require.NoError(t, ValidateReplicaRecoveryMode(batch))
					require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
					encoded, err := json.Marshal(batch)
					require.NoError(t, err)
					transferredBytes += len(encoded)
					transferredRecords += len(batch.Messages)
					require.NoError(t, fp.ReplicaAppendWithMode(batch.Messages, true))
					exchanges++
					require.LessOrEqual(t, exchanges, 3, "one bounded comparison and at most two record batches")
				}
				require.Equal(t, test.count-int(test.want), transferredRecords)
				require.Positive(t, transferredBytes)
				t.Logf("required rollback=%d actual rollback=%d transferred records=%d JSON record-batch bytes=%d exchanges=%d", test.count-test.firstBad, test.count-int(test.want), transferredRecords, transferredBytes, exchanges)
				proofs, err := fp.ReplicaLineageCandidates(uint64(test.count))
				require.NoError(t, err)
				matched, err := sp.VerifyReplicaLineage(proofs[0], uint64(test.count))
				require.NoError(t, err)
				require.True(t, matched)
			})
		}
	}
}
