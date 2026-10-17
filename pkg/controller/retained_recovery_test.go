package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type retainedApplyStorage struct {
	*disk.DiskHandler
	afterPrefix func()
}

func (s *retainedApplyStorage) RetainedPrefixContext(ctx context.Context, start, end uint64) (disk.RetainedPrefixResult, error) {
	result, err := s.DiskHandler.RetainedPrefixContext(ctx, start, end)
	if s.afterPrefix != nil {
		s.afterPrefix()
	}
	return result, err
}

type retainedApplyProvider struct {
	*disk.DiskManager
	storage *retainedApplyStorage
}

func (p *retainedApplyProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	p.storage = &retainedApplyStorage{DiskHandler: h.(*disk.DiskHandler)}
	return p.storage, nil
}

func TestApplyRetainedRecoveryRequiresOriginalWitnessAndDurablePrefix(t *testing.T) {
	for _, variant := range []string{"valid", "historical compaction", "unlocked scan", "leader during scan", "generation during scan", "changed proof", "missing witness", "foreign witness", "legacy apply", "changed generation", "capability"} {
		t.Run(variant, func(t *testing.T) {
			var provider *retainedApplyProvider
			handler, state, p, old := indexedApplyFixtureWithProvider(t, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &retainedApplyProvider{DiskManager: dm}
				return provider
			})
			for _, id := range []string{"broker-1", "broker-2"} {
				broker := state.GetBroker(id)
				broker.RecoveryProtocol = fsm.BootstrapRecoveryProtocolVersion
				data, err := json.Marshal(broker)
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
			}
			release := p.BeginReplicationMutation()
			witness, err := p.CaptureReplicaRequestWitness(context.Background(), 2)
			require.NoError(t, err)
			proof, err := p.ReplicaRetainedPrefixContext(context.Background(), 0, 2)
			require.NoError(t, err)
			release()
			request := fsm.ReplicaCatchupRequest{Topic: old.Topic, Partition: old.Partition, BrokerID: old.BrokerID, SourceBroker: old.SourceBroker,
				Leader: old.Leader, LeaderEpoch: old.LeaderEpoch, LifecycleEpoch: old.LifecycleEpoch, NextOffset: 2, CommittedHWM: 3,
				MaxRecords: 1, RecoveryProtocol: 2, RetainedPrefix: &proof.Proof, LocalWitness: witness}
			batch := fsm.ReplicaCatchupBatch{Topic: old.Topic, Partition: old.Partition, BrokerID: old.BrokerID, SourceBroker: old.SourceBroker,
				Leader: old.Leader, LeaderEpoch: old.LeaderEpoch, LifecycleEpoch: old.LifecycleEpoch, StartOffset: 2, EndOffset: 3, CommittedHWM: 3,
				RecoveryProtocol: 2, RetainedPrefix: &proof.Proof, MatchedRetainedPrefix: &proof.Proof, Verified: true,
				Messages: []types.Message{{Offset: 2, Payload: "committed tail"}}}
			switch variant {
			case "unlocked scan", "leader during scan", "generation during scan":
				provider.storage.afterPrefix = func() {
					done := make(chan interface{}, 1)
					go func() {
						done <- state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"observer","addr":"observer:9001","status":"active"}`)})
					}()
					select {
					case result := <-done:
						require.Nil(t, result)
					case <-time.After(3 * time.Second):
						t.Fatal("retained scan blocked metadata application")
					}
					if variant == "leader during scan" {
						meta := state.GetPartitionMetadata("orders-0")
						meta.LeaderEpoch++
						applyPartitionMetadata(t, state, "orders", 0, *meta)
					}
					if variant == "generation during scan" {
						require.NoError(t, p.ApplyReplicaRollback(0))
						require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "new"}, {Offset: 1, Payload: "new"}}, true))
					}
				}
			case "historical compaction":
				require.False(t, config.HasCleanupPolicy(handler.TopicManager.GetTopic("orders").PolicySnapshot().CleanupPolicy, config.CleanupPolicyCompact))
				meta := state.GetPartitionMetadata("orders-0")
				meta.CommittedHWM = 6
				applyPartitionMetadata(t, state, "orders", 0, *meta)
				request.CommittedHWM = 6
				batch.CommittedHWM, batch.EndOffset, batch.Compacted = 6, 6, true
				batch.Messages = []types.Message{{Offset: 4, Payload: "retained tail"}}
			case "changed proof":
				proof.Proof.Digest = strings.Repeat("a", 64)
			case "missing witness":
				request.LocalWitness = nil
			case "foreign witness":
				_, _, foreign, _ := indexedApplyFixture(t)
				release := foreign.BeginReplicationMutation()
				request.LocalWitness, err = foreign.CaptureReplicaRequestWitness(context.Background(), 2)
				require.NoError(t, err)
				release()
			case "changed generation":
				require.NoError(t, p.ApplyReplicaRollback(0))
				require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "new"}, {Offset: 1, Payload: "new"}}, true))
			case "capability":
				broker := state.GetBroker("broker-1")
				broker.RecoveryProtocol = 1
				data, err := json.Marshal(broker)
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
			}
			batch, err = fsm.SealReplicaCatchupBatch(batch)
			require.NoError(t, err)
			if variant == "legacy apply" {
				err = handler.ApplyReplicaCatchup(context.Background(), batch)
			} else {
				completed, applyErr := handler.ApplyReplicaCatchupVerified(context.Background(), request, batch)
				err = applyErr
				if variant == "valid" || variant == "historical compaction" || variant == "unlocked scan" {
					require.NotNil(t, completed)
				} else {
					require.Nil(t, completed)
				}
			}
			if variant == "valid" || variant == "historical compaction" || variant == "unlocked scan" {
				require.NoError(t, err)
				require.Equal(t, batch.EndOffset, p.NextOffset())
				require.Equal(t, batch.EndOffset, p.GetHWM())
				if variant == "historical compaction" {
					retained, proofErr := p.ReplicaRetainedPrefixContext(context.Background(), 0, 6)
					require.NoError(t, proofErr)
					require.True(t, retained.UsedMarkedAuthority)
					messages, readErr := p.ReadMessages(0, 10)
					require.NoError(t, readErr)
					require.Len(t, messages, 3)
					require.Equal(t, uint64(4), messages[2].Offset)
				}
			} else {
				require.Error(t, err)
				require.Equal(t, uint64(2), p.NextOffset(), "rejection must not append or truncate")
			}
		})
	}
}
