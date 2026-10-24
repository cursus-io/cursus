package controller

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryRemoteProofRefreshAndReplacement(t *testing.T) {
	receiver, source, probe, request := metadataCacheFixture(t)
	for range 20 {
		transferAppend(t, source, source.bootstrapSource, request)
	}
	staged := stageMetadataCache(t, receiver, source)
	metadata := metadataCacheRequest(source, 0).Metadata
	plan := &consumerRecoveryPlan{metadata: metadata, missing: []int{0}, staged: staged, sources: staged.Sources()}
	reader := &consumerRecoveryReader{ctx: context.Background(), staged: staged, metadata: metadata, views: make([]*topic.PartitionReadView, len(metadata))}
	history := consumerHistorySource{reader}
	identity, err := history.ConsumerMetadataIndexIdentity(0)
	require.NoError(t, err)
	_, err = reader.ConsumerMetadataIndexIdentity(0)
	require.Error(t, err, "transfer evidence must not become a durable startup index")
	raw, err := history.ReadRawConsumerMetadataIndex(0, 0, 100)
	require.NoError(t, err)
	require.Len(t, raw, 20)
	bytes := probe.bytes
	require.NoError(t, receiver.refreshConsumerHistorySources(context.Background(), plan))
	require.Equal(t, bytes, probe.bytes, "same-end refresh transfers no frames")
	current, err := history.ConsumerMetadataIndexIdentity(0)
	require.NoError(t, err)
	require.Equal(t, identity, current)
	// A missing source generation certificate (including after a source prefix
	// replacement) forces full adoption even at exactly the same committed end.
	source.bootstrapSource.mu.Lock()
	clear(source.bootstrapSource.certificates[:])
	source.bootstrapSource.mu.Unlock()
	require.ErrorContains(t, receiver.refreshConsumerHistorySources(context.Background(), plan), "source replaced")
	require.Greater(t, probe.bytes, bytes)
	require.NoError(t, staged.Close())
	_, err = history.ConsumerMetadataIndexIdentity(0)
	require.Error(t, err, "closed leases cannot authorize publication")
}
