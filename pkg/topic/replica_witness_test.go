package topic

import (
	"context"
	"errors"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplicaTupleAloneDoesNotAuthorizeAdmission(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	p.MarkReplicaCatchupVerified(1, 1, 3)
	require.False(t, p.ReplicaCatchupVerified(1, 1, 3), "tuple setters cannot certify actual receiver verification")
}

func replicaWitness(t *testing.T, p *Partition, repair uint64) (*ReplicaRequestWitness, *ReplicaCatchupWitness) {
	t.Helper()
	release := p.BeginReplicationMutation()
	defer release()
	request, err := p.CaptureReplicaRequestWitness(context.Background(), p.NextOffset())
	require.NoError(t, err)
	require.NoError(t, p.ValidateReplicaRequestWitness(context.Background(), request, p.NextOffset()))
	witness, err := p.CompleteReplicaCatchupVerification(context.Background(), request, "leader", 1, 1, p.NextOffset(), repair)
	require.NoError(t, err)
	return request, witness
}

func rewriteSameTail(t *testing.T, d *disk.DiskHandler) {
	t.Helper()
	require.NoError(t, d.TruncateTo(0))
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "changed earlier record"}, {Topic: "orders", Offset: 1, Payload: "divergent"}, {Topic: "orders", Offset: 2, Payload: "tail"}}))
}

func TestReplicaWitnessRejectsSameTailRewriteAtEveryBoundary(t *testing.T) {
	for _, phase := range []string{"request", "terminal", "mark", "proof", "replacement"} {
		t.Run(phase, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			request, witness := replicaWitness(t, p, 7)
			if phase == "proof" {
				require.NoError(t, p.AcceptReplicaCatchupWitness(witness, "leader", 1, 1, 3))
			}
			if phase == "replacement" {
				other, _, _ := rollbackPartition(t)
				require.Error(t, other.AcceptReplicaCatchupWitness(witness, "leader", 1, 1, 3))
				require.Error(t, other.ValidateReplicaRequestWitness(context.Background(), request, 3))
				return
			}
			rewriteSameTail(t, d)
			require.Equal(t, uint64(3), p.NextOffset())
			require.Equal(t, uint64(3), p.GetHWM())
			switch phase {
			case "request":
				require.ErrorIs(t, p.ValidateReplicaRequestWitness(context.Background(), request, 3), disk.ErrStaleContentGeneration)
			case "terminal":
				_, err := p.CompleteReplicaCatchupVerification(context.Background(), request, "leader", 1, 1, 3, 7)
				require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
			case "mark":
				require.ErrorIs(t, p.AcceptReplicaCatchupWitness(witness, "leader", 1, 1, 3), disk.ErrStaleContentGeneration)
			case "proof":
				require.False(t, p.ReplicaCatchupVerified(1, 1, 3))
				require.Error(t, p.ValidateReplicaCatchupEligibility(1, 1, 3, 7))
			}
		})
	}
}

func TestReplicaWitnessCompletesOnlyItsUnchangedReconciliationMarker(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "newer"}[newer], func(t *testing.T) {
			p, _, _ := rollbackPartition(t)
			failure := errors.New("same-epoch repair pending")
			p.MarkReconciliationPending(failure)
			_, witness := replicaWitness(t, p, 0)
			if newer {
				p.MarkReconciliationPending(failure)
			}
			err := p.AcceptReplicaCatchupWitness(witness, "leader", 1, 1, 3)
			if newer {
				require.Error(t, err)
				require.ErrorIs(t, p.ReconciliationError(), failure)
			} else {
				require.NoError(t, err)
				require.NoError(t, p.ReconciliationError())
				require.True(t, p.ReplicaCatchupVerified(1, 1, 3))
			}
		})
	}
}

func TestReplicaWitnessRejectsNewAttemptRepairGenerationAndUnavailableStorage(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	_, old := replicaWitness(t, p, 7)
	_, current := replicaWitness(t, p, 8)
	require.Error(t, p.AcceptReplicaCatchupWitness(old, "leader", 1, 1, 3))
	require.NoError(t, p.AcceptReplicaCatchupWitness(current, "leader", 1, 1, 3))
	require.Error(t, p.ValidateReplicaCatchupEligibility(1, 1, 3, 7))
	require.NoError(t, p.ValidateReplicaCatchupEligibility(1, 1, 3, 8))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.CaptureReplicaRequestWitness(ctx, 3)
	require.ErrorIs(t, err, context.Canceled)
	p.dh = struct{ types.StorageHandler }{d}
	_, err = p.CaptureReplicaRequestWitness(context.Background(), 3)
	require.ErrorIs(t, err, ErrReplicaSourceUnsupported)
	require.False(t, p.ReplicaCatchupVerified(1, 1, 3))
	p.dh = d
	require.NoError(t, d.Close())
	require.False(t, p.ReplicaCatchupVerified(1, 1, 3))
}

func TestReplicaWitnessIndexViewBypassesOnlyOwnedMarkerAndCannotBootstrap(t *testing.T) {
 p,_,_:=rollbackPartition(t)
 marker:=errors.New("owned reconciliation marker")
 p.MarkReconciliationPending(marker)
 request,_:=replicaWitness(t,p,0)
 release:=p.BeginReplicationMutation()
 defer release()
 var escaped *PartitionReadView
 require.NoError(t,p.WithReplicaCatchupIndexView(context.Background(),request,3,func(v *PartitionReadView)error{
  escaped=v
  require.Equal(t,uint64(3),v.LastStableOffset())
  require.Error(t,v.ValidateBootstrapSource("orders",0,1))
  require.Error(t,v.ValidateBootstrapRecovery("orders",0,1))
  return nil
 }))
 _,err:=escaped.ReplicaCatchupIndexGeneration("orders",0,3);require.Error(t,err)
 require.Error(t,p.WithReplicaCatchupIndexView(context.Background(),request,3,func(*PartitionReadView)error{
  p.MarkReconciliationPending(marker)
  return nil
 }))
 require.ErrorIs(t,p.ReconciliationError(),marker)
 current,err:=p.CaptureReplicaRequestWitness(context.Background(),3);require.NoError(t,err)
 failure:=errors.New("independent recovery error")
 p.setRecoveryError(failure)
 called:=false
 require.ErrorIs(t,p.WithReplicaCatchupIndexView(context.Background(),current,3,func(*PartitionReadView)error{called=true;return nil}),failure)
 require.False(t,called)
}
