package disk

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLineagePrefixContextBoundsAndCancellation(t *testing.T) {
	d, _ := newLineageHandler(t)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "kept"}, {Topic: "orders", Offset: 1, Payload: "tail"}}))
	for _, end := range []uint64{0, 1, 2} {
		proof, err := d.LineagePrefixContext(context.Background(), end)
		require.NoError(t, err)
		matched, err := d.VerifyLineagePrefix(proof, 2)
		require.NoError(t, err)
		require.True(t, matched)
	}
	_, err := d.LineagePrefixContext(context.Background(), 3)
	require.ErrorContains(t, err, "outside retained range")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = d.LineagePrefixContext(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
}
