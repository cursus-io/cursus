package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// A pre-mutation apply failure must not repeat authenticated prefix probes.
// A partial mutation must discard their original witness before another turn.
func TestPrefixRetryRetainsOnlyUnchangedReceiver(t *testing.T) {
	for _, mode := range []string{"unchanged", "partial rollback", "leader epoch"} {
		t.Run(mode, func(t *testing.T) {
			a, b := make([]types.Message, 1001), make([]types.Message, 1001)
			for i := range a {
				a[i] = types.Message{Offset: uint64(i), Payload: "source"}
				b[i] = a[i]
				if i > 0 {
					b[i].Payload = "divergent"
				}
			}
			source, _, _ := indexedControllerFSM(t, a, 1001)
			receiver, p, cfg := indexedControllerFSM(t, b, 1001)
			cc := NewClusterController(context.Background(), cfg, &MockRaftManager{mockFSM: receiver}, nil, "node-2", "node-2:9001")
			fetcher := &indexedSourceFetcher{source: source}
			turn := &replicaTurn{}
			candidate := fsm.ReplicaCatchupCandidate{Topic: "orders", Partition: 0}
			fault := errors.New("controlled apply failure")
			attempts, probes := 0, 0
			apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
				attempts++
				require.NotNil(t, batch.TruncateTo)
				require.Equal(t, uint64(1), *batch.TruncateTo)
				if attempts == 1 {
					probes = len(fetcher.requests)
				}
				if mode == "leader epoch" {
					meta := receiver.GetPartitionMetadata("orders-0")
					meta.LeaderEpoch++
					data, err := json.Marshal(meta)
					require.NoError(t, err)
					require.Nil(t, receiver.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
				}
				if mode == "partial rollback" || attempts == 4 {
					err := p.ApplyReplicaRollbackChecked(ctx, *batch.TruncateTo, r.NextOffset, batch.MatchedPrefix, batch.CommittedHWM, func() error { return p.CheckReplicaRequestGeneration(ctx, r.LocalWitness) })
					require.NoError(t, err)
					if mode == "unchanged" {
						return nil, nil
					}
				}
				return nil, fault
			}
			for n := 0; n < 100 && attempts < 4; n++ {
				result := cc.advanceReplicaTurn(context.Background(), candidate, turn, fetcher, apply)
				if result.err != nil {
					require.ErrorIs(t, result.err, fault)
				}
				if mode != "unchanged" && attempts == 1 {
					require.Nil(t, turn.request, "a changed storage witness cannot resume")
					if mode == "partial rollback" {
						require.Equal(t, uint64(1), p.NextOffset())
					} else {
						require.Equal(t, uint64(1001), p.NextOffset())
					}
					return
				}
			}
			require.Equal(t, 4, attempts)
			require.Greater(t, probes, 1)
			require.Equal(t, probes+3, len(fetcher.requests), "three failed applies may retry the final challenge, not repeat binary search")
			require.Equal(t, uint64(1), p.NextOffset())
			require.NotContains(t, receiver.GetPartitionMetadata("orders-0").ISR, "node-2")
		})
	}
}
