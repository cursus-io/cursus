package disk

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

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
