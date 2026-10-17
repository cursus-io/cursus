package topic

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// Observe the operation blocked inside its existing lock, rather than relying
// on a sleep to guess whether its optimistic pending check already ran.
func waitForQueuedPartitionMutation(t *testing.T, method string) {
	t.Helper()
	require.Eventually(t, func() bool {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "(*Partition)."+method+"(") && strings.Contains(goroutine, "semaphore.(*Weighted).Acquire(") {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "%s did not queue on its partition lock", method)
}

func TestBootstrapPendingRecheckedAfterQueuedMutation(t *testing.T) {
	tests := []struct {
		name      string
		reconcile bool
		reject    bool
		snapshot  bool
		apply     func(*Partition) error
	}{
		{name: "ReconcileSnapshotHWM", reconcile: true, apply: func(p *Partition) error { return p.ReconcileSnapshotHWM(8) }},
		{name: "FinalizeSnapshotRecovery", reconcile: true, snapshot: true, apply: func(p *Partition) error { return p.FinalizeSnapshotRecovery(8) }},
		{name: "ApplyReplicaHWM", apply: func(p *Partition) error { return p.ApplyReplicaHWM(8) }},
		{name: "SetHWM", apply: func(p *Partition) error { p.SetHWM(8); return nil }},
		{name: "ApplyReplicaRollback", reconcile: true, reject: true, apply: func(p *Partition) error { return p.ApplyReplicaRollback(8) }},
		{name: "ReconcileCommittedHWM", reconcile: true, reject: true, apply: func(p *Partition) error { return p.ReconcileCommittedHWM(8) }},
		{name: "TruncateReplicaTail", reconcile: true, reject: true, apply: func(p *Partition) error { return p.TruncateReplicaTail(8) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			p := NewPartition(0, "orders", &visibilityBenchmarkStorage{messages: make([]types.Message, 8)}, nil, cfg)
			t.Cleanup(p.Close)
			p.HWM = 1
			p.hwmAuthoritative = false
			p.snapshotRecovery = tc.snapshot
			p.recoveryCheckpointHWM = 1
			p.recoverySnapshotHWM = 1
			p.hwmCheckpointCh = make(chan struct{}, 1)
			if tc.reconcile {
				p.reconcileMu.Lock()
			} else {
				p.mu.Lock()
			}
			unlocked := false
			unlock := func() {
				if unlocked {
					return
				}
				unlocked = true
				if tc.reconcile {
					p.reconcileMu.Unlock()
				} else {
					p.mu.Unlock()
				}
			}
			defer unlock()
			done := make(chan error, 1)
			go func() { done <- tc.apply(p) }()
			waitForQueuedPartitionMutation(t, tc.name)
			// The bootstrap session has established durable pending state by the time
			// this queued normal operation finally acquires its lock.
			p.bootstrapPending.Store(true)
			unlock()
			select {
			case err := <-done:
				if tc.reject {
					require.ErrorIs(t, err, ErrBootstrapPending)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("queued mutation did not finish")
			}
			p.mu.RLock()
			hwm, authoritative, snapshot, checkpoint, recovered := p.HWM, p.hwmAuthoritative, p.snapshotRecovery, p.recoveryCheckpointHWM, p.recoverySnapshotHWM
			p.mu.RUnlock()
			require.Equal(t, uint64(1), hwm)
			require.False(t, authoritative)
			require.Equal(t, tc.snapshot, snapshot)
			require.Equal(t, uint64(1), checkpoint)
			require.Equal(t, uint64(1), recovered)
			require.Equal(t, uint64(8), p.NextOffset())
			require.Empty(t, p.hwmCheckpointCh)
			require.True(t, p.BootstrapPending())
		})
	}
}
