package topic

import (
	"context"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDetachedCleanupReadsRawBoundedEvidenceAndGaps(t *testing.T) {
	ctx := context.Background()
	budget := NewDetachedMetadataBudget()
	s := metadataStore(t, metadataRoot(t), budget, 100, []types.DiskMessage{metadataRecord(3, "pending", 1, ""), metadataRecord(97, "pending", 1, types.TransactionMarkerAbort)})
	l := metadataLease(t, s)
	page, err := l.ReadCleanupPage(ctx, 0, 1, 1)
	var limit *types.ReplayByteBudget
	require.ErrorAs(t, err, &limit)
	require.Zero(t, page.Next)
	require.Greater(t, limit.Size, 1)
	page, err = l.ReadCleanupPage(ctx, 0, 1, limit.Size)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, metadataOffsets(page.Messages))
	require.Equal(t, uint64(4), page.Next)
	require.Equal(t, 1, page.Records)
	page, err = l.ReadCleanupPage(ctx, page.Next, 1, 1<<20)
	require.NoError(t, err)
	require.Equal(t, []uint64{97}, metadataOffsets(page.Messages))
	require.Equal(t, uint64(100), page.Next)
	require.Greater(t, budget.Work().ReadBytes, uint64(0))
	require.NoError(t, l.Close())
	_, err = l.ReadCleanupPage(ctx, 0, 1, 1<<20)
	require.Error(t, err)
}
