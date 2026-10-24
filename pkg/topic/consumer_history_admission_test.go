package topic

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryInstallationNeverWaitsForReplication(t *testing.T) {
	for _, gate := range []string{"replication decision", "read view"} {
		t.Run(gate, func(t *testing.T) {
			p := &Partition{topic: config.ConsumerOffsetsTopicName}
			var release func()
			if gate == "replication decision" {
				release = p.BeginReplicationMutation()
			} else {
				p.visibilityMu.RLock()
				release = p.visibilityMu.RUnlock
			}
			defer release()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := p.WithConsumerHistoryInstallation(ctx, func(*PartitionReadView, func(disk.ConsumerHistorySegment, disk.ConsumerHistoryRemovalCertificate) (disk.CompactionResult, error)) error {
				t.Fatal("busy publication must not enter validation or storage")
				return nil
			})
			require.ErrorIs(t, err, ErrConsumerHistoryBusy)
			require.NoError(t, ctx.Err(), "must release the caller's FSM fence before the writer can finish")
			// Both failed acquisitions must release any earlier partition gate.
			if gate == "read view" {
				require.True(t, p.reconcileMu.TryLock())
				p.reconcileMu.Unlock()
			}
		})
	}
}
