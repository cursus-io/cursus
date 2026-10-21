package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTryConsumerSnapshotRefusesContentionWithoutPartialProof(t *testing.T) {
	for _, lock := range []string{"authority", "map", "group", "baseline-map", "baseline-group", "provenance"} {
		t.Run(lock, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("orders", "workers", 1))
			wantGroups, wantProof, err := c.ExportStateWithRecovery()
			require.NoError(t, err)
			var unlock func()
			switch lock {
			case "authority":
				c.authorityMu.Lock()
				unlock = c.authorityMu.Unlock
			case "map":
				c.mu.Lock()
				unlock = c.mu.Unlock
			case "group":
				c.groups["workers"].mu.Lock()
				unlock = c.groups["workers"].mu.Unlock
			case "baseline-map":
				c.raftCompatibility.mu.Lock()
				unlock = c.raftCompatibility.mu.Unlock
			case "baseline-group":
				c.raftCompatibility.groups["workers"].mu.Lock()
				unlock = c.raftCompatibility.groups["workers"].mu.Unlock
			case "provenance":
				c.provenanceMu.Lock()
				unlock = c.provenanceMu.Unlock
			}
			type result struct {
				groups map[string]*GroupStateSnapshot
				proof  *ConsumerRecoveryEnvelope
				err    error
			}
			done := make(chan result, 1)
			go func() {
				groups, proof, err := c.TryExportStateWithRecovery()
				done <- result{groups, proof, err}
			}()
			var got result
			select {
			case got = <-done:
				unlock()
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("consumer snapshot waited for a contended lock")
			}
			require.ErrorIs(t, got.err, ErrConsumerSnapshotBusy)
			require.Nil(t, got.groups)
			require.Nil(t, got.proof)
			groups, proof, err := c.TryExportStateWithRecovery()
			require.NoError(t, err)
			require.Equal(t, wantGroups, groups)
			require.Equal(t, wantProof, proof)
			groups["workers"].Partitions[0] = 99
			proof.RaftCompatibilityState["workers"].Partitions[0] = 99
			groups, proof, err = c.ExportStateWithRecovery()
			require.NoError(t, err)
			require.Equal(t, wantGroups, groups, "live snapshot must be detached")
			require.Equal(t, wantProof, proof, "recovery proof must be detached")
		})
	}
}
