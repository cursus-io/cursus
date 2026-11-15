package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func newEmptyISRRecoveryFSM(t *testing.T, hwm uint64) *BrokerFSM {
	t.Helper()
	f := NewBrokerFSM(nil, nil)
	f.SetDefaultMinISR(2)
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	f.topicState[definition.Name] = &definition
	for index, brokerID := range []string{"node-1", "node-2", "node-3"} {
		f.brokers[brokerID] = &BrokerInfo{
			ID: brokerID, Status: "active", RecoveryProtocol: RecoveryProtocolVersionCurrent,
			IncarnationID: brokerID + "-process", IncarnationEpoch: uint64(index + 1),
		}
	}
	f.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "node-3", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: hwm, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2", "node-3"}, ISR: []string{},
		RecoveryReplicas: []string{"node-1", "node-2", "node-3"},
	}
	return f
}

func emptyISRLineageProof(f *BrokerFSM, brokerID string, hwm uint64, digest string) ISRCatchupProof {
	broker := f.brokers[brokerID]
	return ISRCatchupProof{
		Topic: "orders", Partition: 0, BrokerID: brokerID,
		CommittedHWM: hwm, LocalLEO: hwm, LocalHWM: hwm,
		LeaderEpoch: 7, LifecycleEpoch: topic.InitialLifecycleEpoch,
		IncarnationID: broker.IncarnationID, IncarnationEpoch: broker.IncarnationEpoch,
		RecoveryProtocol: IndexedRecoveryProtocolVersion,
		LineagePrefix:    &disk.LineagePrefix{StartOffset: 0, EndOffset: hwm, Digest: digest},
	}
}

func applyEmptyISRRecovery(t *testing.T, f *BrokerFSM, command ISREmptyRecoveryCommand) interface{} {
	t.Helper()
	data, err := json.Marshal(command)
	require.NoError(t, err)
	return f.Apply(&raft.Log{Data: append([]byte("ISR_EMPTY_RECOVER:"), data...)})
}

func TestEmptyISRRecoveryRequiresMatchingMinISRAndAppliesAtomically(t *testing.T) {
	f := newEmptyISRRecoveryFSM(t, 0)
	digest := strings.Repeat("a", 64)
	proofs := []ISRCatchupProof{
		emptyISRLineageProof(f, "node-2", 0, digest),
		emptyISRLineageProof(f, "node-1", 0, digest),
	}
	_, ready, err := f.BuildISREmptyRecoveryCommand(proofs[:1])
	require.NoError(t, err)
	require.False(t, ready)

	command, ready, err := f.BuildISREmptyRecoveryCommand(proofs)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, []string{"node-1", "node-2"}, []string{command.Proofs[0].BrokerID, command.Proofs[1].BrokerID})
	require.Nil(t, applyEmptyISRRecovery(t, f, command))

	metadata := f.GetPartitionMetadata("orders-0")
	require.Equal(t, []string{"node-1", "node-2"}, metadata.ISR)
	require.Equal(t, "node-1", metadata.Leader)
	require.Equal(t, 8, metadata.LeaderEpoch)
	require.Zero(t, metadata.CommittedHWM)
	require.Equal(t, []string{"node-3"}, metadata.RecoveryReplicas)

	// Retrying after an unknown apply outcome is an exact no-op.
	require.Nil(t, applyEmptyISRRecovery(t, f, command))
	require.Equal(t, 8, f.GetPartitionMetadata("orders-0").LeaderEpoch)
	// The remaining replica uses the ordinary proof-gated catch-up admission.
	remaining := ISRCatchupProof{
		Topic: "orders", Partition: 0, BrokerID: "node-3", CommittedHWM: 0,
		LocalLEO: 0, LocalHWM: 0, LeaderEpoch: 8, LifecycleEpoch: topic.InitialLifecycleEpoch,
	}
	data, err := json.Marshal(remaining)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("ISR_CATCHUP:"), data...)}))
	require.Equal(t, []string{"node-1", "node-2", "node-3"}, f.GetPartitionMetadata("orders-0").ISR)
	// The same retry remains a no-op after ordinary ISR expansion.
	require.Nil(t, applyEmptyISRRecovery(t, f, command))
	require.Equal(t, 8, f.GetPartitionMetadata("orders-0").LeaderEpoch)
}

func TestEmptyISRRecoveryRejectsDivergentAndStaleProofs(t *testing.T) {
	digest := strings.Repeat("b", 64)
	tests := []struct {
		name   string
		mutate func(*ISRCatchupProof)
	}{
		{name: "committed HWM", mutate: func(p *ISRCatchupProof) { p.CommittedHWM-- }},
		{name: "LEO below", mutate: func(p *ISRCatchupProof) { p.LocalLEO-- }},
		{name: "LEO above", mutate: func(p *ISRCatchupProof) { p.LocalLEO++ }},
		{name: "local HWM", mutate: func(p *ISRCatchupProof) { p.LocalHWM-- }},
		{name: "leader epoch", mutate: func(p *ISRCatchupProof) { p.LeaderEpoch++ }},
		{name: "lifecycle epoch", mutate: func(p *ISRCatchupProof) { p.LifecycleEpoch++ }},
		{name: "incarnation", mutate: func(p *ISRCatchupProof) { p.IncarnationEpoch++ }},
		{name: "prefix end", mutate: func(p *ISRCatchupProof) { p.LineagePrefix.EndOffset-- }},
		{name: "digest", mutate: func(p *ISRCatchupProof) { p.LineagePrefix.Digest = strings.Repeat("c", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newEmptyISRRecoveryFSM(t, 2)
			valid := emptyISRLineageProof(f, "node-1", 2, digest)
			candidate := emptyISRLineageProof(f, "node-2", 2, digest)
			test.mutate(&candidate)
			_, ready, err := f.BuildISREmptyRecoveryCommand([]ISRCatchupProof{valid, candidate})
			require.NoError(t, err)
			require.False(t, ready)
			require.Empty(t, f.GetPartitionMetadata("orders-0").ISR)
		})
	}
}

func TestEmptyISRRecoveryRejectsMixedLineageQuorum(t *testing.T) {
	f := newEmptyISRRecoveryFSM(t, 2)
	_, ready, err := f.BuildISREmptyRecoveryCommand([]ISRCatchupProof{
		emptyISRLineageProof(f, "node-1", 2, strings.Repeat("a", 64)),
		emptyISRLineageProof(f, "node-2", 2, strings.Repeat("b", 64)),
	})
	require.NoError(t, err)
	require.False(t, ready)
}

func TestSingleCatchupProofCannotRecoverEmptyISR(t *testing.T) {
	f := newEmptyISRRecoveryFSM(t, 0)
	proof := emptyISRLineageProof(f, "node-1", 0, strings.Repeat("a", 64))
	proof.LineagePrefix = nil
	proof.RecoveryProtocol = 0
	data, err := json.Marshal(proof)
	require.NoError(t, err)
	result := f.Apply(&raft.Log{Data: append([]byte("ISR_CATCHUP:"), data...)})
	require.ErrorContains(t, result.(error), "quorum proof recovery is required")
	require.Empty(t, f.GetPartitionMetadata("orders-0").ISR)
}

func TestEmptyISRRecoveryRetainedPrefix(t *testing.T) {
	f := newEmptyISRRecoveryFSM(t, 3)
	proofs := make([]ISRCatchupProof, 0, 2)
	for _, brokerID := range []string{"node-1", "node-2"} {
		proof := emptyISRLineageProof(f, brokerID, 3, strings.Repeat("a", 64))
		proof.LineagePrefix = nil
		proof.RecoveryProtocol = BootstrapRecoveryProtocolVersion
		proof.RetainedPrefix = &disk.RetainedPrefix{Version: 1, Start: 1, End: 3, Digest: strings.Repeat("d", 64)}
		proofs = append(proofs, proof)
	}
	command, ready, err := f.BuildISREmptyRecoveryCommand(proofs)
	require.NoError(t, err)
	require.True(t, ready)
	require.Nil(t, applyEmptyISRRecovery(t, f, command))
	require.Equal(t, []string{"node-1", "node-2"}, f.GetPartitionMetadata("orders-0").ISR)
}

func TestEmptyISRRecoverySurvivesSnapshotAndReplay(t *testing.T) {
	source := newEmptyISRRecoveryFSM(t, 2)
	digest := strings.Repeat("e", 64)
	command, ready, err := source.BuildISREmptyRecoveryCommand([]ISRCatchupProof{
		emptyISRLineageProof(source, "node-1", 2, digest),
		emptyISRLineageProof(source, "node-2", 2, digest),
	})
	require.NoError(t, err)
	require.True(t, ready)

	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	var encoded bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &encoded}))

	restored := NewBrokerFSM(nil, nil)
	restored.SetDefaultMinISR(2)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	require.Empty(t, restored.GetPartitionMetadata("orders-0").ISR)
	require.Nil(t, applyEmptyISRRecovery(t, restored, command))
	require.Equal(t, []string{"node-1", "node-2"}, restored.GetPartitionMetadata("orders-0").ISR)
	require.Equal(t, 8, restored.GetPartitionMetadata("orders-0").LeaderEpoch)

	recoveredSnapshot, err := restored.Snapshot()
	require.NoError(t, err)
	defer recoveredSnapshot.Release()
	encoded.Reset()
	require.NoError(t, recoveredSnapshot.Persist(&MockSnapshotSink{Writer: &encoded}))
	replayed := NewBrokerFSM(nil, nil)
	replayed.SetDefaultMinISR(2)
	require.NoError(t, replayed.Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	require.Nil(t, applyEmptyISRRecovery(t, replayed, command))
	require.Equal(t, 8, replayed.GetPartitionMetadata("orders-0").LeaderEpoch)
}

func TestBuildEmptyISRRecoveryProofsForZeroAndDataHWM(t *testing.T) {
	for _, test := range []struct {
		name string
		hwm  uint64
	}{
		{name: "zero", hwm: 0},
		{name: "data", hwm: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = true
			cfg.DiskMinFreeBytes = 1
			cfg.DiskMinFreePercent = 0.1
			dm := disk.NewDiskManager(cfg)
			topicManager := topic.NewTopicManager(cfg, dm, nil)
			t.Cleanup(func() { topicManager.Stop(); dm.CloseAllHandlers() })
			require.NoError(t, topicManager.CreateTopic("orders", 1, false, true))
			partition, err := topicManager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			if test.hwm > 0 {
				require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
					{Offset: 0, Payload: "zero", LeaderEpoch: 7},
					{Offset: 1, Payload: "one", LeaderEpoch: 7},
				}, true))
				require.NoError(t, partition.ApplyReplicaHWM(test.hwm))
				partition.FlushDisk()
			}
			f := NewBrokerFSM(topicManager, nil)
			definition := topicManager.GetTopic("orders").Definition()
			f.topicState[definition.Name] = &definition
			f.brokers["node-1"] = &BrokerInfo{
				ID: "node-1", Status: "active", RecoveryProtocol: RecoveryProtocolVersionCurrent,
				IncarnationID: "node-1-process", IncarnationEpoch: 4,
			}
			f.partitionMetadata["orders-0"] = &PartitionMetadata{
				Leader: "node-1", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
				CommittedHWM: test.hwm, CommittedHWMKnown: true, PartitionCount: 1,
				Replicas: []string{"node-1", "node-2", "node-3"}, ISR: []string{},
			}

			proofs, err := f.BuildEmptyISRRecoveryProofsContext(context.Background(), "node-1")
			require.NoError(t, err)
			require.Len(t, proofs, 1)
			proof := proofs[0]
			require.Equal(t, test.hwm, proof.LocalLEO)
			require.Equal(t, test.hwm, proof.LocalHWM)
			require.Equal(t, test.hwm, proof.LineagePrefix.EndOffset)
			require.Len(t, proof.LineagePrefix.Digest, 64)
			require.NoError(t, f.ValidateEmptyISRRecoveryProof(proof))
		})
	}
}
