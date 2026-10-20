package disk

import (
	"context"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestScanRetainedRangeCertifiesTrailingHoles(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 8, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2, Payload: "retained"}}))
	var offsets []uint64
	require.NoError(t, d.ScanRetainedRange(context.Background(), 0, 8, func(m types.Message) error { offsets = append(offsets, m.Offset); return nil }))
	require.Equal(t, []uint64{2}, offsets)
}

func TestScanRetainedRangeRejectsUnmarkedTruncation(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
	require.NoError(t, err)
	require.NoError(t, os.Truncate(d.GetSegmentPath(0), 0))
	require.Error(t, d.ScanRetainedRange(context.Background(), 0, 1, func(types.Message) error { return nil }))
}

func TestScanRetainedRangeDoesNotExtendGapAuthorityAcrossSegments(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 5, nil))
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "unmarked"})
	require.NoError(t, err)
	require.NoError(t, os.Truncate(d.GetSegmentPath(5), 0))
	require.Error(t, d.ScanRetainedRange(context.Background(), 0, 6, func(types.Message) error { return nil }))
}
