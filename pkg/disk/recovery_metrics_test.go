package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRecoveryMetricsIncludeRejectedInstallationChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint")
	data := []byte("durable data")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	before := recoveryMetric(t, "checksum", "hash_bytes")
	require.ErrorContains(t, validateCompactedRangeFile(path, int64(len(data)), "wrong"), "checksum")
	require.Equal(t, float64(len(data)), recoveryMetric(t, "checksum", "hash_bytes")-before)
}

func TestRecoveryMetricsRetainCanceledLineageWork(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 1200)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: "record"}
	}
	require.NoError(t, d.WriteBatchSync(batch))
	read := recoveryMetric(t, "lineage", "read_bytes")
	written := recoveryMetric(t, "lineage", "write_bytes")
	hashed := recoveryMetric(t, "lineage", "canonical_hash_bytes")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.lineageSyncFn = func(file *os.File) error {
		err := file.Sync()
		cancel()
		return err
	}
	_, err := d.MatchLineageCandidatesContext(ctx, nil, 1200)
	d.lineageSyncFn = nil
	require.ErrorIs(t, err, context.Canceled)
	require.Greater(t, recoveryMetric(t, "lineage", "read_bytes"), read)
	require.Greater(t, recoveryMetric(t, "lineage", "write_bytes"), written)
	require.Greater(t, recoveryMetric(t, "lineage", "canonical_hash_bytes"), hashed)
	_, err = d.MatchLineageCandidatesContext(context.Background(), nil, 1200)
	require.NoError(t, err)
	require.Equal(t, uint64(1200), d.lineageProgress.recordsHashed)
}

func recoveryMetric(t *testing.T, stage, unit string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "cursus_recovery_work_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["stage"] == stage && labels["unit"] == unit {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestRecoveryMetricsCountFailedPartialExport(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteBatchSync(detachedCacheRecords(0, 2, 100)))
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	before := recoveryMetric(t, "retained_export", "write_bytes")
	reads := recoveryMetric(t, "retained_page", "read_bytes")
	failed := errors.New("partial write")
	ops := defaultRetainedExportIO()
	ops.write = func(file *os.File, data []byte) (int, error) {
		n, err := file.Write(data[:2])
		return n, errors.Join(err, failed)
	}
	export, err := d.exportRetainedBootstrap(context.Background(), token, 0, 0, 2, bootstrapTestFence(1, 2), ops)
	require.ErrorIs(t, err, failed)
	require.Nil(t, export)
	require.Equal(t, float64(2), recoveryMetric(t, "retained_export", "write_bytes")-before)
	require.Greater(t, recoveryMetric(t, "retained_page", "read_bytes"), reads)
}
