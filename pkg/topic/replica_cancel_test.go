package topic

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/util/contextlock"
	"github.com/stretchr/testify/require"
)

func TestReplicaTailRepairCancelsEveryQueuedPartitionGate(t *testing.T) {
	for blocked := 0; blocked < 5; blocked++ {
		t.Run([]string{"reconcile", "visibility", "hwm-checkpoint", "producer-checkpoint", "state"}[blocked], func(t *testing.T) {
			p := &Partition{}
			gates := []*contextlock.RWMutex{&p.reconcileMu, &p.visibilityMu, &p.hwmCheckpointMu, &p.producerCheckpointMu, &p.mu}
			gates[blocked].Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- p.TruncateReplicaTailContext(ctx, 0) }()
			select {
			case err := <-done:
				gates[blocked].Unlock()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				gates[blocked].Unlock()
				<-done
				t.Fatal("canceled repair retained queued partition gates")
			}
			for _, gate := range gates {
				require.True(t, gate.TryLock(), "earlier gates must be released on cancellation")
				gate.Unlock()
			}
		})
	}
}
