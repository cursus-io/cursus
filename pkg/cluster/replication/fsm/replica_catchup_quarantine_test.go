package fsm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func quarantineRequestFixture(t *testing.T) (*BrokerFSM, ReplicaCatchupRequest, ReplicaCatchupBatch) {
	t.Helper()
	f, _ := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: "anchor"}})
	for _, member := range f.brokers {
		member.RecoveryProtocol = BootstrapRecoveryProtocolVersion
		member.LifecycleProtocol = BrokerProtocolVersionCurrent
	}
	f.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	f.partitionMetadata["orders-0"].CommittedHWM = 3
	request, ready, err := f.BuildReplicaCatchupRequestContext(context.Background(), "node-2", "orders", 0)
	require.NoError(t, err)
	require.True(t, ready)
	batch, err := SealReplicaCatchupBatch(ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch,
		SourceBroker: request.SourceBroker, StartOffset: request.NextOffset, EndOffset: 3,
		CommittedHWM: 3, Compacted: true, Messages: []types.Message{{Offset: 2, Payload: "suffix"}},
	})
	require.NoError(t, err)
	return f, request, batch
}

// The prepared decision must be fenced again when Raft applies it, even though
// building it releases all gates before submission and never mutates storage.
func TestCatchupQuarantineDecisionRevalidatesAtRaftApply(t *testing.T) {
	for _, change := range []string{"none", "leader", "hwm", "assignment"} {
		t.Run(change, func(t *testing.T) {
			f, request, batch := quarantineRequestFixture(t)
			command, err := f.BuildReplicaCatchupQuarantine(context.Background(), request, batch)
			require.NoError(t, err)
			require.NotNil(t, command)
			require.Equal(t, []string{"node-1", "node-2"}, command.ExpectedISR)
			meta := f.partitionMetadata["orders-0"]
			switch change {
			case "leader":
				meta.LeaderEpoch++
			case "hwm":
				meta.CommittedHWM++
			case "assignment":
				meta.Replicas = append(meta.Replicas, "node-3")
			}
			data, err := json.Marshal(command)
			require.NoError(t, err)
			result := f.Apply(&raft.Log{Data: append([]byte("ISR_QUARANTINE:"), data...)})
			if change == "none" {
				require.Nil(t, result)
				require.NotContains(t, meta.ISR, "node-2")
				require.Contains(t, meta.RecoveryReplicas, "node-2")
			} else {
				require.Error(t, result.(error))
				require.Contains(t, meta.ISR, "node-2")
				require.Empty(t, meta.RecoveryReplicas)
			}
			require.Equal(t, uint64(1), request.localAuthority.partition.NextOffset())
		})
	}
}

func TestCatchupQuarantineRejectsChangedResponseAndWitness(t *testing.T) {
	for _, change := range []string{"response_hwm", "response_source", "witness", "authority", "canceled", "ordinary"} {
		t.Run(change, func(t *testing.T) {
			f, request, batch := quarantineRequestFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "response_hwm":
				batch.CommittedHWM++
			case "response_source":
				batch.SourceBroker = "node-3"
			case "witness":
				request.LocalWitness = nil
			case "authority":
				request.localAuthority = nil
			case "canceled":
				cancel()
			case "ordinary":
				batch.Compacted = false
			}
			command, err := f.BuildReplicaCatchupQuarantine(ctx, request, batch)
			if change == "ordinary" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Nil(t, command)
			require.Contains(t, f.GetPartitionMetadata("orders-0").ISR, "node-2")
		})
	}
}
