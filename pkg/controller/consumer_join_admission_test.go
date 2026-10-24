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

func TestConsumerHistoryRoutingAdmissionRejection(t *testing.T) {
	require.Equal(t, coordinator.ErrConsumerMetadataRecovering.Error(), formatCoordinatorRouteError(coordinator.ErrConsumerMetadataRecovering))
	require.Equal(t, coordinatorUnavailableResponse, formatCoordinatorRouteError(fmt.Errorf("persistence: %w", coordinator.ErrConsumerMetadataRecovering)))
	require.Equal(t, coordinatorUnavailableResponse, formatCoordinatorRouteError(ErrConsumerRecoveryPending))
}

func TestTransactionEpochReadPreservesMaintenanceRejection(t *testing.T) {
	_, err := parseTransactionGroupRegistrationEpoch(coordinator.ErrConsumerMetadataRecovering.Error())
	require.ErrorIs(t, err, coordinator.ErrConsumerMetadataRecovering)
	epoch, err := parseTransactionGroupRegistrationEpoch("OK offset=9 found=true registration_epoch=7")
	require.NoError(t, err)
	require.Equal(t, uint64(7), epoch)
	for _, response := range []string{"OK offset=9 found=true", "OK registration_epoch=0", "ERROR: group_not_found", "ERROR: transaction_sync_failed reason=consumer_metadata_recovering"} {
		_, err = parseTransactionGroupRegistrationEpoch(response)
		require.Error(t, err)
		require.NotErrorIs(t, err, coordinator.ErrConsumerMetadataRecovering)
	}
}
