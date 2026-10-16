package topic

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestReplicaSnapshotDetachedPublicationFences(t *testing.T) {
	for _, change := range []string{"none", "owner", "generation", "attempt", "marker", "epoch", "hwm", "lifecycle", "resolver", "decision", "cancel", "post-save"} {
		t.Run(change, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			p.MarkReconciliationPending(errors.New("pending"))
			_, witness := replicaWitness(t, p, 0)
			manager := transaction.NewManager()
			p.SetTransactionDecisionResolver(manager)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var proof *ReplicaSnapshotPublication
			var escaped *SnapshotReadCut
			require.NoError(t, p.WithReplicaSnapshotCertification(ctx, witness, "leader", 1, 1, 3, func(c *SnapshotReadCut) error {
				escaped = c
				var err error
				proof, err = c.PrepareReplicaSnapshotPublication(ctx, true)
				return err
			}))
			_, err := escaped.PrepareReplicaSnapshotPublication(ctx, true)
			require.Error(t, err)
			epoch, hwm, life := 1, uint64(3), uint64(1)
			switch change {
			case "owner":
				p, _, _ = rollbackPartition(t)
			case "generation":
				rewriteSameTail(t, d)
			case "attempt":
				_, _ = replicaWitness(t, p, 0)
			case "marker":
				p.MarkReconciliationPending(errors.New("changed"))
			case "epoch":
				epoch++
			case "hwm":
				hwm--
			case "lifecycle":
				life++
			case "resolver":
				p.SetTransactionDecisionResolver(transaction.NewManager())
			case "decision":
				_, _, err = manager.InitProducer("unrelated")
				require.NoError(t, err)
			case "cancel":
				cancel()
			}
			called := 0
			var escapedGuard func() error
			err = p.WithReplicaSnapshotPublication(ctx, proof, witness, "leader", epoch, life, hwm, func(validate func() error) error {
				escapedGuard = validate
				require.NoError(t, validate())
				called++
				if change == "post-save" {
					_, _, err := manager.InitProducer("unrelated")
					require.NoError(t, err)
				}
				return nil
			})
			if change == "none" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if change == "none" || change == "post-save" {
				require.Equal(t, 1, called)
				require.ErrorContains(t, escapedGuard(), "guard expired")
			} else {
				require.Zero(t, called)
			}
			require.ErrorContains(t, p.WithReplicaSnapshotPublication(context.Background(), proof, witness, "leader", 1, 1, 3, func(func() error) error { t.Fatal("reused proof published"); return nil }), "consumed")
		})
	}
}

func TestReplicaSnapshotProofRejectsWrongPurpose(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		_, err := c.PrepareReplicaSnapshotPublication(context.Background(), false)
		require.ErrorContains(t, err, "actual replica cut")
		return nil
	}))
	require.NoError(t, p.WithBootstrapSource(func(s *BootstrapSource) error {
		c, err := s.SnapshotReadCut()
		require.NoError(t, err)
		_, err = c.PrepareReplicaSnapshotPublication(context.Background(), false)
		require.ErrorContains(t, err, "actual replica cut")
		return nil
	}))
}
