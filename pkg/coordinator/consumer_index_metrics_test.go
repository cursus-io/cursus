package coordinator

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func consumerIndexWork(t *testing.T, unit string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "cursus_recovery_work_total" {
			continue
		}
		for _, m := range family.Metric {
			labels := map[string]string{}
			for _, l := range m.Label {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["stage"] == "consumer_index" && labels["unit"] == unit {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

func TestConsumerIndexCountsFailedPublicationAndCorruptRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index")
	before := consumerIndexWork(t, "write_bytes")
	for range 2 {
		_, err := writeConsumerReplayIndex(path, []byte("written-but-not-published"), func(string) error { return errors.New("crash") })
		require.Error(t, err)
	}
	require.Equal(t, float64(2*len("written-but-not-published")), consumerIndexWork(t, "write_bytes")-before)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
	c, reader := newIndexFixture(t, 10)
	path = filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
	require.NoError(t, os.WriteFile(path, []byte("{broken"), 0600))
	before = consumerIndexWork(t, "read_bytes")
	misses, hits := consumerIndexWork(t, "cache_misses"), consumerIndexWork(t, "cache_hits")
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.False(t, prepared.status.IndexHit)
	require.Equal(t, misses+1, consumerIndexWork(t, "cache_misses"))
	require.Equal(t, float64(len("{broken")), consumerIndexWork(t, "read_bytes")-before)
	prepared, err = c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.True(t, prepared.status.IndexHit)
	require.Equal(t, hits+1, consumerIndexWork(t, "cache_hits"))
	require.Greater(t, consumerIndexWork(t, "read_bytes")-before, float64(len("{broken")), "a later hit must not erase failed work")
}
