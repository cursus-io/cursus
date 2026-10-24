package fsm

import (
	"context"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func prefixRefinementPair(t *testing.T, common, total int) (*BrokerFSM, *BrokerFSM, ReplicaCatchupRequest) {
	t.Helper()
	a, b := make([]types.Message, total), make([]types.Message, total)
	for i := range a {
		a[i] = types.Message{Offset: uint64(i), LeaderEpoch: 4, Payload: fmt.Sprintf("committed-%d", i)}
		b[i] = a[i]
		if i >= common {
			b[i].Payload = fmt.Sprintf("divergent-%d", i)
		}
	}
	source, _ := indexedRecoveryFSM(t, a)
	receiver, _ := indexedRecoveryFSM(t, b)
	for _, state := range []*BrokerFSM{source, receiver} {
		for _, broker := range state.brokers {
			broker.RecoveryProtocol = IndexedRecoveryProtocolVersion
		}
	}
	r, ok, err := receiver.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ok)
	return source, receiver, r
}

func TestReplicaPrefixRefinementPreservesEveryCommonRecord(t *testing.T) {
	for _, common := range []int{0, 1, 17, 64, 99, 100} {
		t.Run(fmt.Sprint(common), func(t *testing.T) {
			source, receiver, request := prefixRefinementPair(t, common, 101)
			original := request
			p := request.localAuthority.partition
			for exchanges := 1; ; exchanges++ {
				require.LessOrEqual(t, exchanges, 8)
				batch, err := source.FetchReplicaCatchup(request)
				require.NoError(t, err)
				require.Equal(t, uint64(101), p.NextOffset(), "no mutation while discovering the boundary")
				require.Equal(t, uint64(101), p.GetHWM())
				_, needed := ReplicaPrefixRefinement(request, batch)
				if !needed {
					require.NotNil(t, batch.TruncateTo)
					require.Equal(t, uint64(common), *batch.TruncateTo)
					break
				}
				request, err = receiver.RefineReplicaPrefixContext(context.Background(), request, batch)
				require.NoError(t, err)
				require.Same(t, original.LocalWitness, request.LocalWitness)
				require.Equal(t, original.NextOffset, request.NextOffset)
				require.LessOrEqual(t, len(request.PrefixCandidates), 4)
			}
		})
	}
}

func TestReplicaPrefixRefinementRejectsChangedAuthorityAndStorage(t *testing.T) {
	for _, change := range []string{"leader", "lifecycle", "incarnation", "capability", "storage", "cancel"} {
		t.Run(change, func(t *testing.T) {
			source, receiver, request := prefixRefinementPair(t, 1, 101)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := request.localAuthority.partition
			require.NoError(t, receiver.ValidateReplicaPrefixContinuationContext(context.Background(), request))
			switch change {
			case "leader":
				receiver.partitionMetadata["orders-0"].LeaderEpoch++
			case "lifecycle":
				receiver.partitionMetadata["orders-0"].LifecycleEpoch++
			case "incarnation":
				receiver.brokers["node-1"].IncarnationID = "new-process"
			case "capability":
				receiver.brokers["node-1"].RecoveryProtocol = 0
			case "storage":
				require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 101, Payload: "changed"}}))
			case "cancel":
				cancel()
			}
			leo, hwm := p.NextOffset(), p.GetHWM()
			_, err = receiver.RefineReplicaPrefixContext(ctx, request, batch)
			require.Error(t, err)
			require.Error(t, receiver.ValidateReplicaPrefixContinuationContext(ctx, request))
			require.Equal(t, leo, p.NextOffset())
			require.Equal(t, hwm, p.GetHWM())
		})
	}
}

func TestReplicaPrefixContinuationRequiresLocalProof(t *testing.T) {
	_, receiver, original := prefixRefinementPair(t, 1, 3)
	for _, missing := range []string{"indexed protocol", "local authority", "storage witness"} {
		t.Run(missing, func(t *testing.T) {
			request := original
			switch missing {
			case "indexed protocol":
				request.RecoveryProtocol = 0
			case "local authority":
				request.localAuthority = nil
			case "storage witness":
				request.LocalWitness = nil
			}
			require.ErrorContains(t, receiver.ValidateReplicaPrefixContinuationContext(context.Background(), request), "original recovery witness")
			require.Equal(t, uint64(3), original.localAuthority.partition.NextOffset())
		})
	}
}
