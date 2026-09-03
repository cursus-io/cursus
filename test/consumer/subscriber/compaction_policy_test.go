package subscriber

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCleanupPolicyFromMetadata(t *testing.T) {
	policy, err := cleanupPolicyFromMetadata("OK topic=state leaders=broker-1:9001 cleanup_policy=delete,compact")
	require.NoError(t, err)
	require.Equal(t, "delete,compact", policy)

	policy, err = cleanupPolicyFromMetadata("OK topic=state leaders=broker-1:9001")
	require.NoError(t, err)
	require.Empty(t, policy)

	_, err = cleanupPolicyFromMetadata("ERROR: topic_not_found")
	require.Error(t, err)
}

func TestOmittedCleanupPolicyClearsCompactionClassification(t *testing.T) {
	consumer := &Consumer{}
	consumer.compactionEnabled.Store(true)

	require.NoError(t, consumer.applyTopicPolicyMetadata("OK topic=state leaders=broker-1:9001"))

	require.False(t, consumer.compactionEnabled.Load())
}

func TestCountSkippedOffsetsIncludesInteriorHoles(t *testing.T) {
	require.Equal(t, uint64(4), countSkippedOffsets(0, []types.Message{{Offset: 3}, {Offset: 5}}))
	require.Zero(t, countSkippedOffsets(3, []types.Message{{Offset: 3}, {Offset: 4}}))
}
