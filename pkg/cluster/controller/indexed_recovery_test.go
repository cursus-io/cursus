package controller

import (
	"context"
	"encoding/json"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type indexedSourceFetcher struct {
	source   *fsm.BrokerFSM
	requests []fsm.ReplicaCatchupRequest
}

func (s *indexedSourceFetcher) FetchReplicaCatchup(_ context.Context, _ string, _ int, request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	s.requests = append(s.requests, request)
	request.MaxRecords = 1
	return s.source.FetchReplicaCatchup(request)
}

func TestIndexedRecoveryRefreshesDurableProofAfterEachBatch(t *testing.T) {
	makeFSM := func(messages []types.Message) (*fsm.BrokerFSM, *topic.Partition, *config.Config) {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		cfg.EnabledDistribution = true
		cfg.DiskMinFreeBytes = 1
		cfg.DiskMinFreePercent = 0.1
		dm := disk.NewDiskManager(cfg)
		t.Cleanup(dm.CloseAllHandlers)
		tm := topic.NewTopicManager(cfg, dm, nil)
		state := fsm.NewBrokerFSM(tm, nil)
		for _, id := range []string{"node-1", "node-2"} {
			data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: id + ":9001", Status: "active", RecoveryProtocol: 1})
			require.NoError(t, err)
			require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
		}
		definition := topic.DefaultDefinition("orders", cfg)
		definition.Partitions = 1
		definition.ReplicationFactor = 2
		command, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "node-1"})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), command...)}))
		p, err := tm.GetTopic("orders").GetPartition(0)
		require.NoError(t, err)
		t.Cleanup(p.Close)
		if len(messages) > 0 {
			require.NoError(t, p.ReplicaAppendWithMode(messages, true))
			require.NoError(t, p.ApplyReplicaHWM(uint64(len(messages))))
		}
		metadata := state.GetPartitionMetadata("orders-0")
		metadata.CommittedHWM = 3
		metadata.CommittedHWMKnown = true
		metadata.ISR = []string{"node-1"}
		data, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
		return state, p, cfg
	}
	messages := []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}, {Offset: 2, Payload: "three"}}
	source, _, _ := makeFSM(messages)
	receiver, p, cfg := makeFSM(nil)
	cc := NewClusterController(context.Background(), cfg, &MockRaftManager{mockFSM: receiver}, nil, "node-2", "node-2:9001")
	fetcher := &indexedSourceFetcher{source: source}
	require.NoError(t, cc.RunReplicaCatchupOnce(context.Background(), fetcher, func(_ context.Context, batch fsm.ReplicaCatchupBatch) error {
		require.Nil(t, batch.TruncateTo)
		if err := p.ReplicaAppendWithMode(batch.Messages, true); err != nil {
			return err
		}
		if batch.EndOffset == batch.CommittedHWM {
			return p.ApplyReplicaHWM(batch.CommittedHWM)
		}
		return nil
	}))
	require.Len(t, fetcher.requests, 3)
	for i, request := range fetcher.requests {
		require.Equal(t, uint64(i), request.PrefixCandidates[0].EndOffset)
	}
	require.NotEqual(t, fetcher.requests[0].PrefixCandidates[0].Digest, fetcher.requests[1].PrefixCandidates[0].Digest)
	require.Equal(t, uint64(3), p.NextOffset())
	require.Len(t, receiver.BuildISRCatchupProofs("node-2"), 1)
}

func TestValidateIndexedRecoveryResponseBindsModeAndRequestedPrefix(t *testing.T) {
	prefix := disk.LineagePrefix{StartOffset: 0, EndOffset: 2, Digest: strings.Repeat("a", 64)}
	request := fsm.ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", SourceBroker: "node-1", NextOffset: 2, CommittedHWM: 3, MaxRecords: 1, RecoveryProtocol: 1, PrefixCandidates: []disk.LineagePrefix{prefix}}
	valid := fsm.ReplicaCatchupBatch{Topic: request.Topic, BrokerID: request.BrokerID, Leader: request.Leader, SourceBroker: request.SourceBroker, StartOffset: 2, EndOffset: 3, CommittedHWM: 3, Verified: true, RecoveryProtocol: 1, MatchedPrefix: &prefix, Messages: []types.Message{{Offset: 2, Payload: "next"}}}
	for _, test := range []struct {
		name   string
		mutate func(*fsm.ReplicaCatchupBatch)
	}{
		{"mode downgrade", func(b *fsm.ReplicaCatchupBatch) { b.RecoveryProtocol = 0; b.MatchedPrefix = nil }},
		{"unknown mode", func(b *fsm.ReplicaCatchupBatch) { b.RecoveryProtocol = 2 }},
		{"missing prefix", func(b *fsm.ReplicaCatchupBatch) { b.MatchedPrefix = nil }},
		{"unverified final batch", func(b *fsm.ReplicaCatchupBatch) { b.Verified = false }},
		{"unrequested prefix", func(b *fsm.ReplicaCatchupBatch) {
			p := prefix
			p.Digest = strings.Repeat("b", 64)
			b.MatchedPrefix = &p
		}},
		{"wrong source", func(b *fsm.ReplicaCatchupBatch) { b.SourceBroker = "node-3" }},
		{"stale leader", func(b *fsm.ReplicaCatchupBatch) { b.LeaderEpoch++ }},
		{"stale lifecycle", func(b *fsm.ReplicaCatchupBatch) { b.LifecycleEpoch++ }},
		{"wrong receiver", func(b *fsm.ReplicaCatchupBatch) { b.BrokerID = "node-3" }},
		{"wrong partition", func(b *fsm.ReplicaCatchupBatch) { b.Partition++ }},
		{"unproven rollback", func(b *fsm.ReplicaCatchupBatch) {
			to := uint64(1)
			b.TruncateTo = &to
			b.Messages = nil
			b.Verified = false
			b.EndOffset = b.StartOffset
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			batch := valid
			test.mutate(&batch)
			batch, err := fsm.SealReplicaCatchupBatch(batch)
			require.NoError(t, err)
			require.Error(t, validateReplicaCatchupBatch(request, batch))
		})
	}
	sealed, err := fsm.SealReplicaCatchupBatch(valid)
	require.NoError(t, err)
	require.NoError(t, validateReplicaCatchupBatch(request, sealed))
	sealed.MatchedPrefix = &disk.LineagePrefix{StartOffset: 0, EndOffset: 2, Digest: strings.Repeat("b", 64)}
	require.ErrorContains(t, validateReplicaCatchupBatch(request, sealed), "checksum")
	valid.RecoveryProtocol = 0
	request.RecoveryProtocol = 0
	sealed, err = fsm.SealReplicaCatchupBatch(valid)
	require.NoError(t, err)
	require.ErrorContains(t, validateReplicaCatchupBatch(request, sealed), "unexpected indexed")
}

func TestValidateIndexedRecoveryAcceptsNewerCommittedHWM(t *testing.T) {
	prefix := disk.LineagePrefix{EndOffset: 1, Digest: strings.Repeat("a", 64)}
	request := fsm.ReplicaCatchupRequest{NextOffset: 1, CommittedHWM: 1, MaxRecords: 1, RecoveryProtocol: 1, PrefixCandidates: []disk.LineagePrefix{prefix}}
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{StartOffset: 1, EndOffset: 2, CommittedHWM: 2, Verified: true, RecoveryProtocol: 1, MatchedPrefix: &prefix, Messages: []types.Message{{Offset: 1}}})
	require.NoError(t, err)
	require.NoError(t, validateReplicaCatchupBatch(request, batch))
}
