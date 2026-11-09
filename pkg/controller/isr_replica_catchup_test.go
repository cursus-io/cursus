package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/require"
)

// TestISRReplicaCatchupFillsCommittedGap reproduces a restarted follower whose
// local committed suffix is behind metadata while its ISR membership survives.
func TestISRReplicaCatchupFillsCommittedGap(t *testing.T) {
	for _, protocol := range []int{0, fsm.BootstrapRecoveryProtocolVersion} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			h, state, p, d := receiverWitnessFixture(t, 1, 2, protocol, false)
			_, source, _, _ := receiverWitnessFixture(t, 2, 2, protocol, false)
			for _, member := range []*fsm.BrokerFSM{state, source} {
				meta := member.GetPartitionMetadata("orders-0")
				meta.ISR = []string{"source", "receiver"}
				applyPartitionMetadata(t, member, "orders", 0, *meta)
			}
			require.ErrorContains(t, state.ReconcileReplicaMaterializations("receiver"), "local replica has not reached committed HWM")
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			require.Zero(t, request.RecoveryProtocol, "an existing ISR member uses incremental catch-up")
			require.Equal(t, uint64(1), request.NextOffset)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.Nil(t, batch.TruncateTo)
			require.Len(t, batch.Messages, 1, "transfer only the missing suffix")
			encoded, err := json.Marshal(batch)
			require.NoError(t, err)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			require.NotNil(t, witness)
			require.Equal(t, uint64(2), p.NextOffset())
			require.Equal(t, uint64(2), p.GetHWM())
			require.NoError(t, state.ReconcileReplicaMaterializations("receiver"))
			require.Equal(t, []string{"source", "receiver"}, state.GetPartitionMetadata("orders-0").ISR)
			_, _, _, pending := state.ReplicaSchedulingIdentity("receiver", fsm.ReplicaCatchupCandidate{Topic: "orders", Partition: 0})
			require.False(t, pending, "completed catch-up must leave the recovery queue")
			records, err := d.ReadMessages(0, 2)
			require.NoError(t, err)
			require.Len(t, records, 2)
			require.Equal(t, "record-0", records[0].Payload)
			require.Equal(t, "record-1", records[1].Payload)
			t.Logf("gap_records=%d payload_bytes=%d serialized_batch_bytes=%d", len(batch.Messages), len(batch.Messages[0].Payload), len(encoded))
		})
	}
}

// TestISRReplicaRecoveryFenceRejectsDestructiveOrStaleBatches keeps the ISR
// exception limited to current, append-only, uncompacted legacy replication.
func TestISRReplicaRecoveryFenceRejectsDestructiveOrStaleBatches(t *testing.T) {
	for _, mode := range []string{"truncate", "compacted", "leader", "lifecycle"} {
		t.Run(mode, func(t *testing.T) {
			_, state, p, _ := receiverWitnessFixture(t, 1, 2, 0, false)
			_, source, _, _ := receiverWitnessFixture(t, 2, 2, 0, false)
			for _, member := range []*fsm.BrokerFSM{state, source} {
				meta := member.GetPartitionMetadata("orders-0")
				meta.ISR = []string{"source", "receiver"}
				applyPartitionMetadata(t, member, "orders", 0, *meta)
			}
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			switch mode {
			case "truncate":
				offset := uint64(0)
				batch.TruncateTo = &offset
			case "compacted":
				batch.Compacted = true
			case "leader":
				batch.LeaderEpoch++
			case "lifecycle":
				batch.LifecycleEpoch++
			}
			require.NoError(t, fsm.ValidateReplicaRecoveryMode(batch), "exercise the receiver fence, not mode parsing")
			called := false
			require.Error(t, state.WithReplicaRequestRecoveryFence(request, batch, func() error { called = true; return nil }))
			require.False(t, called)
			require.Equal(t, uint64(1), p.NextOffset())
			require.Equal(t, uint64(1), p.GetHWM())
		})
	}
}
