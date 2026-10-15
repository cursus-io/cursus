package eventsource

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type countedRecoveryStorage struct {
	*disk.DiskHandler
	records, bytes atomic.Int64
}

func (s *countedRecoveryStorage) ReadMessagesBounded(offset uint64, max, maxBytes int, oversized bool) ([]types.Message, int, error) {
	records, bytes, err := s.DiskHandler.ReadMessagesBounded(offset, max, maxBytes, oversized)
	s.records.Add(int64(len(records)))
	s.bytes.Add(int64(bytes))
	return records, bytes, err
}

type countedRecoveryProvider struct{ storage *countedRecoveryStorage }

func (p *countedRecoveryProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return p.storage, nil
}

func TestReplicaRecoveryIndexReusesGenerationAndReadsOnlyAppendedSuffix(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 1, 0.1
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	storage := &countedRecoveryStorage{DiskHandler: d}
	tm := topic.NewTopicManager(cfg, &countedRecoveryProvider{storage}, nil)
	require.NoError(t, tm.CreateTopic("orders", 1, false, true))
	h := NewHandler(tm)
	t.Cleanup(func() { require.NoError(t, h.Close()); tm.Stop(); require.NoError(t, d.Close()) })
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	records := make([]types.Message, 128)
	for i := range records {
		records[i] = types.Message{Offset: uint64(i), Key: "stream", AggregateVersion: uint64(i + 1), Payload: strings.Repeat("x", 1024)}
	}
	require.NoError(t, p.ReplicaAppend(records))
	require.NoError(t, p.ApplyReplicaHWM(128))
	index := func(end uint64) {
		release := p.BeginReplicationMutation()
		defer release()
		witness, err := p.CaptureReplicaRequestWitness(context.Background(), end)
		require.NoError(t, err)
		require.NoError(t, p.WithReplicaCatchupIndexView(context.Background(), witness, end, func(v *topic.PartitionReadView) error { return h.IndexReplicaCatchupWithView("orders", 0, end, v) }))
	}
	storage.records.Store(0)
	storage.bytes.Store(0)
	index(128)
	require.Equal(t, int64(128), storage.records.Load())
	require.Positive(t, storage.bytes.Load())
	firstBytes := storage.bytes.Load()
	storage.records.Store(0)
	storage.bytes.Store(0)
	index(128)
	require.Zero(t, storage.records.Load())
	require.Zero(t, storage.bytes.Load(), "unchanged terminal verification must not scan index history")
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 128, Key: "stream", AggregateVersion: 129, Payload: strings.Repeat("y", 1024)}}))
	require.NoError(t, p.ApplyReplicaHWM(129))
	index(129)
	require.Equal(t, int64(1), storage.records.Load())
	require.Less(t, storage.bytes.Load(), firstBytes)
	t.Logf("initial index read %d records/%d bytes; same generation repeat 0; append suffix 1 record/%d bytes", 128, firstBytes, storage.bytes.Load())
	require.Equal(t, "OK version=129", h.HandleStreamVersion("STREAM_VERSION topic=orders key=stream"))
}

func TestReplicaRecoveryIndexCertificateInvalidatedByPartialReplayFailure(t *testing.T) {
	for _, name := range []string{"incremental", "explicit_reset", "replica_catchup"} {
		t.Run(name, func(t *testing.T) {
			h, p := newBootstrapHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "stream", AggregateVersion: 1, Payload: "first"}}))
			require.NoError(t, p.ApplyReplicaHWM(1))
			release := p.BeginReplicationMutation()
			witness, err := p.CaptureReplicaRequestWitness(context.Background(), 1)
			require.NoError(t, err)
			err = p.WithReplicaCatchupIndexView(context.Background(), witness, 1, func(v *topic.PartitionReadView) error { return h.IndexReplicaCatchupWithView("orders", 0, 1, v) })
			release()
			require.NoError(t, err)
			h.mu.RLock()
			idx := h.indexes["orders:0"]
			_, certified := h.replicaIndexGenerations["orders:0"]
			h.mu.RUnlock()
			require.True(t, certified)
			// Replay changes the index before discovering the later version gap.
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 1, Key: "stream", AggregateVersion: 2, Payload: "valid suffix"}, {Offset: 2, Key: "stream", AggregateVersion: 4, Payload: "gap"}}))
			require.NoError(t, p.ApplyReplicaHWM(3))
			switch name {
			case "explicit_reset":
				err = h.RecoverIndexFromLog("orders", 0, idx)
			case "replica_catchup":
				release := p.BeginReplicationMutation()
				witness, captureErr := p.CaptureReplicaRequestWitness(context.Background(), 3)
				require.NoError(t, captureErr)
				err = p.WithReplicaCatchupIndexView(context.Background(), witness, 3, func(v *topic.PartitionReadView) error { return h.IndexReplicaCatchupWithView("orders", 0, 3, v) })
				release()
			default:
				err = h.IndexCommittedToHWM("orders", 0, 3)
			}
			require.ErrorContains(t, err, "stream index gap")
			_, versionErr := idx.CheckedVersion("stream")
			require.ErrorIs(t, versionErr, ErrStreamIndexInvalid, "partial derived-state mutation is never readable")
			h.mu.RLock()
			_, certified = h.replicaIndexGenerations["orders:0"]
			_, cached := h.indexes["orders:0"]
			h.mu.RUnlock()
			require.False(t, cached, "a partially rebuilt index must be retired")
			require.False(t, certified, "a partially changed index cannot retain its recovery certificate")
			require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=stream version=3 message=unsafe"), "stream index gap", "the next append must recover from the log, not continue from partial version 2")
			require.Equal(t, uint64(3), p.NextOffset())
		})
	}
}
