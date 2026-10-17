package fsm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplicaSnapshotFenceCurrentAuthority(t *testing.T) {
	for _, change := range []string{"none", "leader", "epoch", "lifecycle", "source-isr", "source-config", "source-inactive", "receiver-isr", "receiver-config", "receiver-inactive", "hwm-unknown", "hwm-regressed"} {
		t.Run(change, func(t *testing.T) {
			f, _ := indexedRecoveryFSM(t, nil)
			request := f.BuildReplicaCatchupRequests("node-2")[0]
			request.CommittedHWM = 1
			meta := f.partitionMetadata["orders-0"]
			meta.CommittedHWM = 1
			switch change {
			case "leader":
				meta.Leader = "other"
			case "epoch":
				meta.LeaderEpoch++
			case "lifecycle":
				meta.LifecycleEpoch++
			case "source-isr":
				meta.ISR = nil
			case "source-config":
				meta.Replicas = []string{"node-2"}
			case "source-inactive":
				f.brokers[request.SourceBroker].Status = "inactive"
			case "receiver-isr":
				meta.ISR = append(meta.ISR, "node-2")
			case "receiver-config":
				meta.Replicas = []string{request.SourceBroker}
			case "receiver-inactive":
				f.brokers["node-2"].Status = "inactive"
			case "hwm-unknown":
				meta.CommittedHWMKnown = false
			case "hwm-regressed":
				meta.CommittedHWM = 0
			}
			called := false
			err := f.WithReplicaSnapshotRecoveryFence(request, func() error {
				called = true
				require.False(t, f.transitionMu.TryLock())
				require.False(t, f.materializationMu.TryLock())
				return nil
			})
			if change == "none" {
				require.NoError(t, err)
				require.True(t, called)
			} else {
				require.Error(t, err)
				require.False(t, called)
			}
		})
	}
}
