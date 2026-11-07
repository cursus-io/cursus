package e2e_cluster

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestHistoryAdmissionRetryPreservesLifecycleBlockers(t *testing.T) {
	loading := coordinator.ErrConsumerMetadataRecovering.Error()
	for _, code := range []string{"topic_delete_blocked", "topic_truncate_blocked"} {
		t.Run(code, func(t *testing.T) {
			for _, reason := range []string{
				"topic delete blocked: " + loading,
				"topic delete blocked: refresh replicated consumer groups: " + loading,
			} {
				require.True(t, isHistoryAdmissionRejection(&wire.BrokerError{Code: code, Fields: map[string]string{"reason": reason}}))
			}
			for _, reason := range []string{
				"",
				"topic delete blocked: consumer group has active members",
				"topic delete blocked: consumer group retains subscriptions outside topic",
				"topic delete blocked: active transaction references topic",
				"topic delete blocked: refresh replicated consumer groups: unexpected EOF",
				"topic delete blocked: consumer group \"" + loading + "\" has active members",
			} {
				require.False(t, isHistoryAdmissionRejection(&wire.BrokerError{Code: code, Fields: map[string]string{"reason": reason}}), reason)
			}
		})
	}
	for _, code := range []string{"topic_revision_conflict", "illegal_generation", "unknown_member_id", "delete_topic_failed"} {
		require.False(t, isHistoryAdmissionRejection(&wire.BrokerError{Code: code, Fields: map[string]string{"reason": loading}}), code)
	}
	require.True(t, isHistoryAdmissionRejection(&wire.BrokerError{Code: "consumer_metadata_recovering"}))
}
