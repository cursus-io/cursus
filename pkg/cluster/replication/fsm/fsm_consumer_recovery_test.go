package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type authorityTopicHandler struct{}

func (*authorityTopicHandler) Publish(string, *types.Message) error      { return nil }
func (*authorityTopicHandler) CreateTopic(string, int, bool, bool) error { return nil }
func authorityCoordinator(t *testing.T) *coordinator.Coordinator {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c := coordinator.NewCoordinator(context.Background(), cfg, &authorityTopicHandler{})
	t.Cleanup(c.Stop)
	return c
}
func persistAuthorityFSM(t *testing.T, f *BrokerFSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	require.NoError(t, err)
	var b bytes.Buffer
	require.NoError(t, snap.Persist(&MockSnapshotSink{Writer: &b}))
	return b.Bytes()
}

func TestLiveConsumerRestoreRejectsUnknownBeforeMutation(t *testing.T) {
	c := authorityCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "keep", 1))
	f := NewBrokerFSM(nil, c)
	before := c.ExportState()
	raw := persistAuthorityFSM(t, NewBrokerFSM(nil, nil))
	require.ErrorIs(t, f.Restore(io.NopCloser(bytes.NewReader(raw))), coordinator.ErrConsumerMetadataBootstrapUnknown)
	require.Equal(t, before, c.ExportState())
	require.False(t, c.ConsumerRecoveryRequirement().Pending())
}

func TestLiveConsumerRestoreLeavesPersistentRequirement(t *testing.T) {
	c := authorityCoordinator(t)
	f := NewBrokerFSM(nil, c)
	raw := persistAuthorityFSM(t, NewBrokerFSM(nil, authorityCoordinator(t)))
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(raw))))
	require.True(t, c.ConsumerRecoveryRequirement().Pending())
	require.False(t, c.ConsumerRecoveryRequirement().Installing)
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
}

func TestConsumerRecoveryFenceDoesNotWaitForBusyMaterialization(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	f.materializationMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- f.WithConsumerMetadataRecoveryFence(func([]PartitionMetadata) error {
			t.Error("busy fence called recovery")
			return nil
		})
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		f.materializationMu.Unlock()
	case <-time.After(time.Second):
		f.materializationMu.Unlock()
		<-done
		t.Fatal("recovery waited while owning its outer admission lease")
	}
}
func TestConsumerRecoveryFSMPairedSnapshotAndEmptyRestore(t *testing.T) {
	c := authorityCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	require.NoError(t, c.ApplyOffsetUpdateFromFSM("g", "events", []coordinator.OffsetItem{{Partition: 0, Offset: 5}}))
	c.SetRoutedOffsetRecordWriter(func(r coordinator.ConsumerMetadataRecord) error {
		c.RecordConsumerMetadataSource(r.Group, 3)
		return nil
	})
	require.NoError(t, c.CommitOffset("g", "events", 0, 100))
	raw := persistAuthorityFSM(t, NewBrokerFSM(nil, c))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(raw, &state))
	require.NotNil(t, state.ConsumerRecovery)
	require.Equal(t, uint64(5), state.ConsumerRecovery.RaftCompatibilityState["g"].Offsets["events"][0])
	peer := authorityCoordinator(t)
	f := NewBrokerFSM(nil, peer)
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(raw))))
	require.NoError(t, peer.ValidateConsumerMetadataBootstrapEligibility(3))
	live, proof, err := peer.ExportStateWithRecovery()
	require.NoError(t, err)
	require.Equal(t, uint64(100), live["g"].Offsets["events"][0])
	require.Equal(t, []int{3}, proof.SourcePartitions["g"])
	// Invalid authority must fail before either live state or FSM state mutates.
	state.ConsumerRecovery.Version++
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.Error(t, f.Restore(io.NopCloser(bytes.NewReader(invalid))))
	require.Equal(t, live, peer.ExportState())
	// A present proven-empty pair replaces existing state, despite omitted groupState.
	empty := authorityCoordinator(t)
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(persistAuthorityFSM(t, NewBrokerFSM(nil, empty))))))
	require.Empty(t, peer.ExportState())
	require.NoError(t, peer.ValidateConsumerMetadataBootstrapEligibility(0))
	// Only an original startup may accept a legacy empty snapshot.
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	peer, err = coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &authorityTopicHandler{})
	require.NoError(t, err)
	t.Cleanup(peer.Stop)
	f = NewBrokerFSM(nil, peer)
	raw = persistAuthorityFSM(t, NewBrokerFSM(nil, nil))
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(raw))))
	require.ErrorIs(t, peer.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(persistAuthorityFSM(t, f)))))
	require.ErrorIs(t, peer.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
}
func TestConsumerRecoveryFSMGroupCommandsNeverPublish(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	require.NoError(t, tm.CreateTopic("events", 1, false, false))
	c := authorityCoordinator(t)
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
		t.Fatal("GROUP_SYNC attempted publication")
		return nil
	})
	f := NewBrokerFSM(tm, c)
	for _, body := range []string{
		`{"type":"REGISTER","group":"g","topic":"events","partition_count":1}`,
		`{"type":"JOIN","group":"g","member":"a","topic":"events","partition_count":1}`,
		`{"type":"LEAVE","group":"g","member":"a","generation":1}`,
		`{"type":"REGISTER","group":"sub","topics":["events"],"partition_counts":{"events":1}}`,
		`{"type":"JOIN","group":"sub","member":"b"}`,
		`{"type":"EXPIRE","group":"sub","members":["b"],"generation":1}`,
	} {
		require.Nil(t, f.Apply(&raft.Log{Data: []byte("GROUP_SYNC:" + body)}))
	}
	_, proof, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	require.True(t, proof.KnownComplete)
	require.Equal(t, 2, proof.RaftCompatibilityState["g"].Generation)
	require.Equal(t, 2, proof.RaftCompatibilityState["sub"].Generation)
}
