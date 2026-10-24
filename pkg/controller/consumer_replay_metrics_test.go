package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type partialConsumerReplay struct{ stagedConsumerTestReader }

func (partialConsumerReplay) ReadCommittedConsumerMetadata(string, int, uint64, int) ([]types.Message, error) {
	return []types.Message{{Key: "key", Payload: "data"}}, errors.New("partial read")
}

func TestConsumerReplayCountsRepeatedPartialReads(t *testing.T) {
	value := func() float64 {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, f := range families {
			if f.GetName() != "cursus_recovery_work_total" {
				continue
			}
			for _, m := range f.Metric {
				labels := map[string]string{}
				for _, l := range m.Label {
					labels[l.GetName()] = l.GetValue()
				}
				if labels["stage"] == "consumer_replay" && labels["unit"] == "decoded_bytes" {
					return m.Counter.GetValue()
				}
			}
		}
		return 0
	}
	r := &consumerRecoveryReader{ctx: context.Background(), measureReplay: true, views: make([]*topic.PartitionReadView, 1), staged: &partialConsumerReplay{}}
	before := value()
	for range 2 {
		_, err := r.ReadCommittedConsumerMetadata("", 0, 0, 1)
		require.Error(t, err)
	}
	require.Equal(t, float64(14), value()-before)
	r.measureReplay = false
	_, _ = r.ReadCommittedConsumerMetadata("", 0, 0, 1)
	require.Equal(t, float64(14), value()-before, "maintenance proof reads have their own counters")
}
