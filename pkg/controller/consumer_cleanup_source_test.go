package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type stagedCleanupTestReader struct{ *stagedConsumerTestReader }

func (r stagedCleanupTestReader) ConsumerHistoryIdentity(int) (string, error) {
	return "certified-prefix", nil
}
func (r stagedCleanupTestReader) ReadRawConsumerHistory(p int, next uint64, max int) ([]types.Message, error) {
	return r.ReadCommittedConsumerMetadata("", p, next, max)
}
func (r stagedCleanupTestReader) ReadConsumerCleanupPage(p int, next uint64, maxRecords, maxBytes int) (types.ReplayPage, error) {
	messages, err := r.ReadRawConsumerHistory(p, next, maxRecords)
	return types.ReplayPage{Messages: messages, Next: r.ends[p], Records: len(messages), Cursor: types.ReplayCursor{Initialized: true}}, err
}

func TestConsumerCleanupStagesBeforeLocksAndFencesRemoteProof(t *testing.T) {
	h, _, _, _ := consumerBootstrapFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	transferRegister(t, state, "remote", "remote-process", "active", 2)
	transferRegister(t, state, "local", "local-process", "active", 2)
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
	m.Replicas, m.ISR, m.Leader = []string{"remote"}, []string{"remote"}, "remote"
	m.LeaderEpoch++
	m.CommittedHWM = 1
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-1:"), data...)}))
	remote := state.GetBroker("remote")
	staged := stagedCleanupTestReader{&stagedConsumerTestReader{ends: []uint64{0, 1, 0, 0}, sources: map[int]ConsumerMetadataRecoverySource{1: {BrokerID: remote.ID, IncarnationID: remote.IncarnationID, IncarnationEpoch: remote.IncarnationEpoch}}, records: map[int][]types.Message{1: {{Offset: 0, Payload: "raw evidence"}}}}}
	before := h.Coordinator.ExportState()
	h.SetConsumerMetadataRecoveryProvider(func(ctx context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
		require.Equal(t, []int{1}, request.Partitions)
		require.Equal(t, before, h.Coordinator.ExportState(), "staging holds no coordinator global lock")
		require.NoError(t, state.WithConsumerMetadataRecoveryFence(func([]fsm.PartitionMetadata) error { return nil }), "staging holds no FSM fence")
		return staged, nil
	})
	reader, closeReader, err := h.prepareConsumerCleanup(context.Background())
	require.NoError(t, err)
	defer closeReader()
	staged.readHook = func() { assertConsumerReadUnderFSMFence(t, state) }
	var bounds []coordinator.ConsumerCleanupPartition
	require.NoError(t, reader.WithConsumerCleanupView(func(view coordinator.ConsumerCleanupView) error {
		bounds = append(bounds, view.Partitions()...)
		require.Equal(t, uint64(1), bounds[1].End)
		require.Empty(t, bounds[1].DurableGeneration, "remote proof cannot become a persisted startup/cleanup cache")
		page, err := view.Read(1, 0, types.ReplayCursor{}, 128, 1<<20)
		require.NoError(t, err)
		require.Equal(t, "raw evidence", page.Messages[0].Payload)
		return nil
	}))
	proof := reader.(*preparedConsumerCleanup)
	require.NoError(t, proof.CheckConsumerCleanupProof(bounds))
	closeReader()
	require.True(t, staged.closed)
	require.Equal(t, before, h.Coordinator.ExportState(), "cleanup evidence must not import state or reset clocks")
	transferRegister(t, state, "remote", "replacement-process", "active", 2)
	require.Error(t, proof.CheckConsumerCleanupProof(bounds), "source process replacement invalidates ephemeral proof")
}
