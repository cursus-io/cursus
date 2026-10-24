package controller

import (
	"fmt"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCooperativeFetchUnavailableCoordinatorIsRetryable(t *testing.T) {
	ch := newCoordinatorRoutingHandler("local", fsm.NewBrokerFSM(nil, nil), nil)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	err := ch.validateCooperativeFetch(CommonArgs{TopicName: "orders", GroupName: "workers", Ownership: &protocol.CooperativeToken{RegistrationEpoch: 1}})
	require.Error(t, err)
	parsed, ok := protocol.ParseErrorResponse(err.Error())
	require.True(t, ok)
	require.Equal(t, "coordinator_not_available", parsed.Code)
	require.True(t, parsed.Retryable)
}
func TestCooperativeFetchCacheSeparatesGroups(t *testing.T) {
	args := CommonArgs{GroupName: "a", Ownership: &protocol.CooperativeToken{RegistrationEpoch: 1}}
	first := consumerOffsetCacheKey("orders", args)
	args.GroupName = "b"
	require.NotEqual(t, first, consumerOffsetCacheKey("orders", args))
}

func TestCooperativeOffsetAppendFailureClassification(t *testing.T) {
	for _, response := range []string{
		`ERROR: broker_error offset=131 reason="forward raft apply to leader: leader unknown"`,
		`ERROR: consumer_route_fenced`,
		`ERROR: insufficient_in_sync_replicas`,
	} {
		parsed, ok := protocol.ParseErrorResponse(formatCoordinatorError(consumerOffsetAppendError(response)))
		require.True(t, ok)
		require.True(t, parsed.Retryable, response)
	}
	for _, response := range []string{`ERROR: invalid_payload`, `ERROR: cooperative_fenced`, `ERROR: internal_topic_write_forbidden`} {
		parsed, ok := protocol.ParseErrorResponse(formatCoordinatorError(consumerOffsetAppendError(response)))
		require.True(t, ok)
		require.False(t, parsed.Retryable, response)
	}
}

func TestCooperativeLifecycleErrorPreservesRetryClassification(t *testing.T) {
	err := fmt.Errorf("persist group lifecycle: %w", consumerOffsetAppendError(`ERROR: broker_error reason="leader unavailable"`))
	parsed, ok := protocol.ParseErrorResponse(formatCoordinatorError(err))
	require.True(t, ok)
	require.True(t, parsed.Retryable)
	require.Equal(t, "coordinator_not_available", parsed.Code)
}

func TestCooperativeDistributedActivationSurvivesLocalFlagDisable(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":"local","incarnation_id":"process","lifecycle_protocol":%d,"recovery_protocol":2}`, fsm.CooperativeConsumerProtocolVersion))}))
	ch := newCoordinatorRoutingHandler("local", state, nil)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	ch.Config.ConsumerCooperativeEnabled = true
	require.False(t, ch.cooperativeEnabled(), "local trigger alone cannot activate distributed membership")
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`COOPERATIVE_CONSUMER_ACTIVATE:{"voters":["local"]}`)}))
	ch.Config.ConsumerCooperativeEnabled = false
	require.True(t, ch.cooperativeEnabled(), "durable activation survives local configuration changes")
	require.Contains(t, ch.handleHelp(), "consumer_cooperative_v1")
}

func TestCooperativeTokenlessFetchToleratesMissingFSMBeforeActivation(t *testing.T) {
	ch := newCoordinatorRoutingHandler("local", nil, nil)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	require.NoError(t, ch.validateCooperativeFetch(CommonArgs{TopicName: "orders", GroupName: "workers"}))
}
