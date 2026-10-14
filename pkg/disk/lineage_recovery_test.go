package disk

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLineageTailRelativeRecoveryOnDisk(t *testing.T) {
	for _, test := range []struct {
		name            string
		count, firstBad int
		lastSame        bool
		want            uint64
	}{
		{"single bad last record", 1025, 1024, false, 1024},
		{"early divergence reconvergent last record", 64, 1, true, 1},
		{"long divergence", 1600, 100, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _ := newLineageHandler(t)
			receiver, _ := newLineageHandler(t)
			original := make([]types.DiskMessage, test.count)
			divergent := make([]types.DiskMessage, test.count)
			for i := range original {
				original[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: fmt.Sprintf("source-%d", i), LeaderEpoch: 7}
				divergent[i] = original[i]
				if i >= test.firstBad && (!test.lastSame || i != test.count-1) {
					divergent[i].Payload = "divergent"
				}
			}
			require.NoError(t, source.WriteBatchSync(original))
			require.NoError(t, receiver.WriteBatchSync(divergent))
			proofs, err := receiver.LineageCandidates(uint64(test.count))
			require.NoError(t, err)
			match, err := source.MatchLineageCandidates(proofs, uint64(test.count))
			require.NoError(t, err)
			require.NotNil(t, match)
			require.Equal(t, test.want, match.EndOffset)
			require.NoError(t, receiver.TruncateTo(match.EndOffset))
			require.NoError(t, receiver.WriteBatchSync(original[match.EndOffset:]))
			proof := lineageTail(t, receiver, uint64(test.count))
			ok, err := source.VerifyLineagePrefix(proof, uint64(test.count))
			require.NoError(t, err)
			require.True(t, ok)
		})
	}
}
