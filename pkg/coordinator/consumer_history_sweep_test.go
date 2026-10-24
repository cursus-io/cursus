package coordinator

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type historySweepReader struct {
	ConsumerMetadataIndexReader
	identity string
}

func (r *historySweepReader) ConsumerMetadataIndexIdentity(p int) (string, error) {
	if p == 0 {
		return r.identity, nil
	}
	return r.ConsumerMetadataIndexReader.ConsumerMetadataIndexIdentity(p)
}

func TestConsumerHistorySweepEveryIntermediateReplay(t *testing.T) {
	for _, mode := range []string{"ordinary", "transaction", "cross_partition_transaction"} {
		t.Run(mode, func(t *testing.T) {
			transactions := mode != "ordinary"
			c, base := newIndexFixture(t, 1000)
			var source ConsumerMetadataIndexReader = base
			if transactions {
				var r *historyTransactionReader
				c, r = historyTransactionFixture(t, 100, types.TransactionMarkerCommit)
				base, source = r.replayIndexTestReader, r
			}
			end := uint64(len(base.messages[0]))
			base.end = map[int]uint64{0: end}
			if mode == "cross_partition_transaction" {
				// Real transaction routing differs from ordinary group routing.
				// Intents contribute provenance but never materialized offsets.
				base.messages[1] = append([]types.Message(nil), base.messages[0][:2]...)
				base.messages[0] = base.messages[0][2:]
				base.end[1] = 2
			}
			reader := &historySweepReader{source, base.identity}
			source = reader
			full, err := c.PrepareFullConsumerMetadataReplay(source)
			require.NoError(t, err)
			whole, err := c.PrepareConsumerHistoryCompaction(source, 0, 0, end)
			require.NoError(t, err)
			sweep, safe := whole.Sweep()
			require.True(t, safe)
			work, _ := whole.ReadWork()
			require.Equal(t, int64(3*end), work)
			for first := uint64(0); first < end; {
				// Single-frame segments deliberately separate intent and marker.
				width := uint64(17)
				if transactions {
					width = 1
				}
				stop := min(first+width, end)
				p, err := sweep.Segment(first, stop)
				require.NoError(t, err)
				require.NoError(t, c.ValidateConsumerHistoryCompaction(source, p))
				applyHistoryPlanToFixture(base, p)
				reader.identity = fmt.Sprintf("own-publication-%d", stop)
				require.NoError(t, sweep.Published(p, reader.identity))
				// A cache miss/restart at EVERY publication observes exact state.
				c.cfg.ConsumerRecoveryIndexEnabled = false
				replay, err := c.PrepareFullConsumerMetadataReplay(source)
				require.NoError(t, err)
				require.Equal(t, full.scratch.ExportState(), replay.scratch.ExportState())
				require.Equal(t, full.scratch.metadataSources, replay.scratch.metadataSources)
				first = stop
			}
			want := 2
			if transactions {
				want = 4
			}
			if mode == "cross_partition_transaction" {
				want = 2
			}
			require.Len(t, base.messages[0], want)
		})
	}
}

func TestConsumerHistorySweepCannotSkipIntentOrAcceptExternalMutation(t *testing.T) {
	c, r := historyTransactionFixture(t, 3, types.TransactionMarkerCommit)
	whole, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 8)
	require.NoError(t, err)
	sweep, ok := whole.Sweep()
	require.True(t, ok)
	_, err = sweep.Segment(3, 4)
	require.Error(t, err, "cannot publish marker before its intent")
	p, err := sweep.Segment(0, 3)
	require.NoError(t, err)
	_, err = sweep.Segment(3, 4)
	require.Error(t, err, "pending installation must be acknowledged first")
	r.identity = "external-bootstrap"
	require.ErrorContains(t, c.ValidateConsumerHistoryCompaction(r, p), "source changed")
}

func TestConsumerHistorySweepDeclinesNonCompositionalHistory(t *testing.T) {
	for _, kind := range []string{"revision_regression", "multiple_sources"} {
		t.Run(kind, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			if kind == "revision_regression" {
				m := r.messages[0][1]
				m.Offset = 11
				r.messages[0] = append(r.messages[0], m)
			} else {
				r.messages[1] = []types.Message{encodedMetadataMessage(t, replayOffset("g", 1, 1, 1), 0)}
			}
			end := uint64(len(r.messages[0]))
			p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, end)
			require.NoError(t, err, "single atomic range still has a full replay proof")
			_, safe := p.Sweep()
			require.False(t, safe)
		})
	}
}
