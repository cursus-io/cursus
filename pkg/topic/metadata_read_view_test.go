package topic

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMetadataReadViewFreezesTransactionMarkerBoundary(t *testing.T) {
	storage := &visibilityBenchmarkStorage{messages: []types.Message{
		{Offset: 0, Payload: "transaction", TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{Offset: 1, Payload: "independent"},
		{Offset: 2, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	}}
	p := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	t.Cleanup(p.Close)
	for _, message := range storage.messages {
		p.indexTransactionMessage(message)
	}
	p.SetHWM(3)
	require.NoError(t, p.WithReadView(func(v *PartitionReadView) error {
		records, err := v.ReadCommittedMetadataThrough(0, 2, 10)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, "independent", records[0].Payload)
		records, err = v.ReadCommittedMetadata(0, 10)
		require.NoError(t, err)
		require.Len(t, records, 2)
		return nil
	}))
}

func TestBootstrapMetadataViewReadsBeyondUnresolvedTransaction(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	receiver := partitionTransfer(t, d, 0, 4, []types.DiskMessage{
		{Topic: "orders", Offset: 0, TransactionalID: "open", Epoch: 1, TransactionState: types.TransactionStateOpen, Payload: "unresolved"},
		{Topic: "orders", Offset: 2, Payload: "independent"},
	})
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	defer s.Close()
	require.False(t, s.Prepared())
	require.Error(t, s.WithPreparedReadView(func(*PartitionReadView) error { t.Fatal("unprepared callback"); return nil }))
	require.NoError(t, s.Install(receiver, nil))
	sentinel := errors.New("retry rebuild")
	var escaped *PartitionReadView
	require.ErrorIs(t, s.Complete(func(v *PartitionReadView, _ []byte) error {
		escaped = v
		records, err := v.ReadCommittedMetadata(0, 10)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, "independent", records[0].Payload)
		records, err = v.ReadCommittedMetadataThrough(0, 2, 10)
		require.NoError(t, err)
		require.Empty(t, records)
		_, err = v.ReadCommittedMetadataThrough(0, 5, 10)
		require.Error(t, err)
		return sentinel
	}), sentinel)
	require.True(t, p.BootstrapPending())
	_, err = escaped.ReadCommittedMetadata(0, 10)
	require.Error(t, err)
	require.NoError(t, s.Complete(nil))
	require.False(t, s.Prepared())
	require.Error(t, s.WithPreparedReadView(func(*PartitionReadView) error { return nil }))
}

func TestPreparedBootstrapReadViewUsesRetainedMetadata(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	receiver := partitionTransfer(t, d, 0, 3, []types.DiskMessage{{Topic: "orders", Offset: 1, Payload: "retained"}})
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Install(receiver, nil))
	// Model the durable prepared state after manifest removal but before a failed
	// directory sync returns. Catalog lookup is impossible in this state.
	require.NoError(t, s.disk.CompleteBootstrap(s.metadata.TransferID))
	s.prepared = true
	p.bootstrapPrepared = true
	require.True(t, s.Prepared())
	_, err = s.disk.PendingBootstrapCatalog()
	require.Error(t, err)
	sentinel := errors.New("callback failed")
	var escaped *PartitionReadView
	require.ErrorIs(t, s.WithPreparedReadView(func(v *PartitionReadView) error {
		escaped = v
		records, err := v.ReadCommittedMetadata(0, 10)
		require.NoError(t, err)
		require.Len(t, records, 1)
		return sentinel
	}), sentinel)
	require.True(t, p.BootstrapPending())
	_, err = escaped.ReadCommittedMetadata(0, 10)
	require.Error(t, err)
	p.bootstrapDefinitionKnown = true
	p.bootstrapLifecycle = 999
	require.False(t, s.Prepared())
	require.Error(t, s.WithPreparedReadView(func(*PartitionReadView) error { return nil }))
	p.bootstrapLifecycle = s.metadata.Fence.LifecycleEpoch
	s.Close()
	require.False(t, s.Prepared())
	require.Error(t, s.WithPreparedReadView(func(*PartitionReadView) error { return nil }))
	s, err = p.BeginBootstrapSession()
	require.NoError(t, err)
	defer s.Close()
	require.True(t, s.Prepared())
	require.NoError(t, s.WithPreparedReadView(func(v *PartitionReadView) error { _, err := v.ReadCommittedMetadata(0, 1); return err }))
	require.NoError(t, s.Complete(nil))
}
