package controller

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/stretchr/testify/require"
)

func TestJoinAdmissionRejectionDistinguishesAmbiguousPersistence(t *testing.T) {
	for _, fallback := range []string{"register_group_failed", "coordinator_not_available"} {
		require.Equal(t, coordinator.ErrConsumerMetadataRecovering.Error(), formatJoinAdmissionError(coordinator.ErrConsumerMetadataRecovering, fallback))
		ambiguous := fmt.Errorf("persist group lifecycle: %w", coordinator.ErrConsumerMetadataRecovering)
		require.Equal(t, fmt.Sprintf("ERROR: %s reason=%q", fallback, ambiguous.Error()), formatJoinAdmissionError(ambiguous, fallback))
	}
}
