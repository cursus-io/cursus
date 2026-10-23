package controller

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/stretchr/testify/require"
)

func TestSnapshotReadLeaseIncludesResponseWriter(t *testing.T) {
	for _, command := range []string{"LIST_SNAPSHOTS", "FETCH_SNAPSHOT"} {
		t.Run(command, func(t *testing.T) {
			cfg := config.DefaultConfig()
			// Test owner slots, independently of the few microseconds needed to
			// refill a full-burst reservation after charging the first response.
			cfg.Recovery.QuantumBytes = 4096
			limits, err := cfg.RecoverySettings().Limits()
			require.NoError(t, err)
			budget, err := recovery.NewBudget(limits, nil)
			require.NoError(t, err)
			h := &CommandHandler{Config: cfg, recoveryBudget: budget}
			ctx, completion := recovery.WithCompletion(context.Background())
			defer completion.Finish()
			calls := 0
			read := func(string) string { calls++; return "OK snapshot" }
			request := command + " topic=test recovery_requester=peer"
			require.Equal(t, "OK snapshot", h.handleBudgetedSnapshotRead(ctx, request, read))
			require.Equal(t, 1, budget.Usage().Active)
			require.Equal(t, "ERROR: recovery_busy", h.handleBudgetedSnapshotRead(ctx, request, read))
			require.Equal(t, 1, calls, "denied reads must not enter storage")
			completion.Finish()
			require.Zero(t, budget.Usage().Active)
			require.Equal(t, "OK snapshot", h.handleBudgetedSnapshotRead(context.Background(), request, read))
			require.Zero(t, budget.Usage().Active)
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			require.Equal(t, "ERROR: recovery_busy", h.handleBudgetedSnapshotRead(canceled, request, read))
			require.Equal(t, 2, calls)
		})
	}
}
