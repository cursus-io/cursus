package topic

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func requireExpiredBootstrapView(t *testing.T, v *PartitionReadView) {
	t.Helper()
	require.Error(t, v.ValidateIdentity("orders", 0))
	require.Error(t, v.ValidateBootstrapSource("orders", 0, 7))
	require.Error(t, v.ValidateBootstrapRecovery("orders", 0, 7))
	require.Zero(t, v.Generation())
	require.Zero(t, v.FirstOffset())
	require.Zero(t, v.CommittedEnd())
	require.Zero(t, v.LastStableOffset())
	_, err := v.ReadMessages(0, 10)
	require.Error(t, err)
	_, _, err = v.ReadMessagesBounded(0, 10, 4096, true)
	require.Error(t, err)
	_, _, next, err := v.ReadCommittedPage(42, 10, 4096, true)
	require.Error(t, err)
	require.Equal(t, uint64(42), next, "an expired view must not advance a cursor")
	_, err = v.ReadCommittedRange(0, 10, 10)
	require.Error(t, err)
	_, err = v.OffsetRange()
	require.Error(t, err)
}

func TestBootstrapSourceViewBoundsAndLifetime(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	p.validateBootstrapDefinition(Definition{Name: "orders", Partitions: 1, LifecycleEpoch: 7})
	require.NoError(t, p.ReplicaAppend([]types.Message{
		{Offset: 0, Payload: "committed"},
		{Offset: 1, Payload: "uncommitted"},
		{Offset: 2, Payload: "uncommitted"},
	}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	require.Equal(t, uint64(3), d.GetFlushedOffset())
	var escaped *BootstrapSource
	callbackFailure := errors.New("catalog capture failed")
	err := p.WithBootstrapSource(func(s *BootstrapSource) error {
		escaped = s
		v := s.View()
		require.NoError(t, v.ValidateBootstrapSource("orders", 0, 7))
		require.Error(t, v.ValidateIdentity("other", 0))
		require.Error(t, v.ValidateIdentity("orders", 1))
		require.Error(t, v.ValidateBootstrapSource("orders", 0, 8))
		require.Error(t, v.ValidateBootstrapRecovery("orders", 0, 7))
		require.Equal(t, uint64(1), v.CommittedEnd())
		require.Equal(t, uint64(1), v.LastStableOffset())
		rangeInfo, err := v.OffsetRange()
		require.NoError(t, err)
		require.Equal(t, PartitionOffsetRange{Earliest: 0, Latest: 1, LEO: 1, HWM: 1}, rangeInfo)
		raw, err := v.ReadMessages(0, 10)
		require.NoError(t, err)
		require.Len(t, raw, 1)
		bounded, _, err := v.ReadMessagesBounded(0, 10, 4096, true)
		require.NoError(t, err)
		require.Equal(t, raw, bounded)
		page, _, next, err := v.ReadCommittedPage(0, 10, 4096, true)
		require.NoError(t, err)
		require.Equal(t, raw, page)
		require.Equal(t, uint64(1), next)
		ranged, err := v.ReadCommittedRange(0, 3, 10)
		require.NoError(t, err)
		require.Equal(t, raw, ranged)
		page, _, next, err = v.ReadCommittedPage(1, 10, 4096, true)
		require.NoError(t, err)
		require.Empty(t, page)
		require.Equal(t, uint64(1), next)
		_, err = s.Export(context.Background(), disk.BootstrapFence{CommittedHWM: 3}, nil)
		require.ErrorContains(t, err, "HWM mismatch")
		return callbackFailure
	})
	require.ErrorIs(t, err, callbackFailure)
	requireExpiredBootstrapView(t, escaped.View())
	requireExpiredBootstrapView(t, nil)
	_, err = escaped.Export(context.Background(), disk.BootstrapFence{CommittedHWM: 1}, nil)
	require.ErrorContains(t, err, "expired")
	require.False(t, p.BootstrapPending())
	// A failed callback releases exclusion without discarding the physical tail.
	require.NoError(t, p.WithReadView(func(v *PartitionReadView) error {
		require.Error(t, v.ValidateBootstrapSource("orders", 0, 7))
		require.Error(t, v.ValidateBootstrapRecovery("orders", 0, 7))
		records, err := v.ReadMessages(0, 10)
		require.NoError(t, err)
		require.Len(t, records, 3)
		return nil
	}))
}

func TestBootstrapRecoveryViewRejectsWrongAuthorityAndExpires(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	p.validateBootstrapDefinition(Definition{Name: "orders", Partitions: 1, LifecycleEpoch: 7})
	r := partitionTransfer(t, d, 0, 3, nil)
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Install(r, nil))
	var escaped *PartitionReadView
	require.NoError(t, s.Complete(func(v *PartitionReadView, _ []byte) error {
		escaped = v
		require.NoError(t, v.ValidateBootstrapRecovery("orders", 0, 7))
		require.Error(t, v.ValidateBootstrapSource("orders", 0, 7))
		require.Error(t, v.ValidateBootstrapRecovery("orders", 0, 8))
		require.Equal(t, uint64(3), v.LastStableOffset())
		rangeInfo, err := v.OffsetRange()
		require.NoError(t, err)
		require.Equal(t, uint64(3), rangeInfo.HWM)
		return nil
	}))
	requireExpiredBootstrapView(t, escaped)
}
