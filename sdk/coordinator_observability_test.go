package sdk

import (
	"net"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCoordinatorMetricsConsumer(t *testing.T, role ConsumerClientRole) *Consumer {
	t.Helper()
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()
	cfg := NewDefaultConsumerConfig()
	cfg.EnableMetrics = true
	cfg.ClientRole = role
	cfg.Topic = "private-topic"
	cfg.GroupID = "private-group"
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	consumer.mu.Lock()
	consumer.memberID = "private-member"
	consumer.mu.Unlock()
	t.Cleanup(consumer.rootCancel)
	return consumer
}

func counterVecValue(t *testing.T, metric interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	value := &dto.Metric{}
	require.NoError(t, metric.Write(value))
	return value.GetCounter().GetValue()
}

func TestInitialBootstrapRedirectFillsCoordinatorCacheAndRetryStabilizesIt(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleProcessor)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	bootstrap := c.trackCoordinatorConnection(client, "bootstrap:9000", coordinatorRouteBootstrap)
	redirect := &BrokerError{
		Code:   "NOT_COORDINATOR",
		Fields: map[string]string{"host": "coordinator-1", "port": "9000"},
	}
	require.True(t, c.handleNotCoordinatorResponse(bootstrap, "FETCH_OFFSET", redirect))

	c.mu.RLock()
	assert.Equal(t, "coordinator-1:9000", c.coordinatorAddr)
	assert.Equal(t, coordinatorRouteRetry, c.coordinatorRouteSource)
	c.mu.RUnlock()

	retry := c.trackCoordinatorConnection(client, "coordinator-1:9000", coordinatorRouteRetry)
	c.recordCoordinatorCommand(retry, "FETCH_OFFSET", coordinatorResultSuccess)
	c.mu.RLock()
	assert.Equal(t, coordinatorRouteCache, c.coordinatorRouteSource)
	c.mu.RUnlock()

	redirectMetric, err := consumerCoordinatorRedirects.GetMetricWithLabelValues("fetch_offset", "processor", "bootstrap")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, redirectMetric))
	refreshMetric, err := consumerCoordinatorCacheRefresh.GetMetricWithLabelValues("success", "redirect", "processor")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, refreshMetric))
	retryMetric, err := consumerCoordinatorRetry.GetMetricWithLabelValues("fetch_offset", "success", "processor")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, retryMetric))
}

func TestStableCachedCoordinatorSuccessDoesNotRecordRedirect(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	c.setCoordinatorCache("coordinator-1:9000", coordinatorRouteCache, coordinatorReasonInitial)
	conn := c.trackCoordinatorConnection(client, "coordinator-1:9000", coordinatorRouteCache)
	c.recordCoordinatorCommand(conn, "HEARTBEAT", coordinatorResultSuccess)

	commandMetric, err := consumerCoordinatorCommands.GetMetricWithLabelValues("heartbeat", "consumer", "cache", "success")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, commandMetric))
	redirectMetric, err := consumerCoordinatorRedirects.GetMetricWithLabelValues("heartbeat", "consumer", "cache")
	require.NoError(t, err)
	assert.Equal(t, float64(0), counterVecValue(t, redirectMetric))
}

func TestRedirectRetryBudgetFailureIsRecorded(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	conn := c.trackCoordinatorConnection(client, "coordinator-2:9000", coordinatorRouteRetry)
	c.recordCoordinatorCommand(conn, "FETCH_OFFSET", coordinatorResultFailure)
	c.logCoordinatorRetryExhausted("FETCH_OFFSET", 5, 5)

	retryMetric, err := consumerCoordinatorRetry.GetMetricWithLabelValues("fetch_offset", "failure", "consumer")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, retryMetric))
	commandMetric, err := consumerCoordinatorCommands.GetMetricWithLabelValues("fetch_offset", "consumer", "retry", "failure")
	require.NoError(t, err)
	assert.Equal(t, float64(1), counterVecValue(t, commandMetric))
}

func TestSameCoordinatorRedirectInvalidatesCacheBeforeRediscovery(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	c.setCoordinatorCache("coordinator-1:9000", coordinatorRouteCache, coordinatorReasonInitial)
	conn := c.trackCoordinatorConnection(client, "coordinator-1:9000", coordinatorRouteCache)
	require.True(t, c.handleNotCoordinatorResponse(conn, "HEARTBEAT", &BrokerError{
		Code:   "NOT_COORDINATOR",
		Fields: map[string]string{"host": "coordinator-1", "port": "9000"},
	}))

	c.mu.RLock()
	assert.Empty(t, c.coordinatorAddr)
	assert.Equal(t, coordinatorReasonRedirect, c.coordinatorPendingReason)
	c.mu.RUnlock()
}

func TestLateRedirectFromSupersededConnectionDoesNotRollBackCache(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	c.setCoordinatorCache("coordinator-2:9000", coordinatorRouteRetry, coordinatorReasonRedirect)
	oldConn := c.trackCoordinatorConnection(client, "coordinator-1:9000", coordinatorRouteCache)
	require.True(t, c.handleNotCoordinatorResponse(oldConn, "HEARTBEAT", &BrokerError{
		Code:   "NOT_COORDINATOR",
		Fields: map[string]string{"host": "coordinator-1", "port": "9000"},
	}))

	c.mu.RLock()
	assert.Equal(t, "coordinator-2:9000", c.coordinatorAddr)
	assert.Equal(t, coordinatorRouteRetry, c.coordinatorRouteSource)
	c.mu.RUnlock()
}

func TestAddresslessLateRedirectFromSupersededConnectionPreservesCache(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	oldConn := c.trackCoordinatorConnection(client, "coordinator-1:9000", coordinatorRouteCache)
	c.setCoordinatorCache("coordinator-2:9000", coordinatorRouteRetry, coordinatorReasonRedirect)
	require.True(t, c.handleNotCoordinatorResponse(oldConn, "HEARTBEAT", &BrokerError{Code: "NOT_COORDINATOR"}))

	c.mu.RLock()
	assert.Equal(t, "coordinator-2:9000", c.coordinatorAddr)
	assert.Equal(t, coordinatorRouteRetry, c.coordinatorRouteSource)
	c.mu.RUnlock()
}

func TestFailedStaleDialCannotInvalidateNewerCoordinatorCache(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	c.setCoordinatorCache("coordinator-2:9000", coordinatorRouteRetry, coordinatorReasonRedirect)

	assert.False(t, c.invalidateCoordinatorCacheIfCurrent("coordinator-1:9000", coordinatorReasonReconnect))
	c.mu.RLock()
	assert.Equal(t, "coordinator-2:9000", c.coordinatorAddr)
	assert.Equal(t, coordinatorRouteRetry, c.coordinatorRouteSource)
	c.mu.RUnlock()
}

func TestReconnectReusesCacheUntilDialOrBrokerProvesItStale(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleCooperative)
	c.setCoordinatorCache("coordinator-2:9000", coordinatorRouteCache, coordinatorReasonInitial)
	c.markCoordinatorReconnect()

	address, source, reason, reconnect := c.coordinatorCacheSnapshot()
	assert.Equal(t, "coordinator-2:9000", address)
	assert.Equal(t, coordinatorRouteCache, source)
	assert.Equal(t, coordinatorReasonCacheMiss, reason)
	assert.True(t, reconnect)

	_, _, _, reconnect = c.coordinatorCacheSnapshot()
	assert.False(t, reconnect, "only the next coordinator dial is classified as reconnect")
}

func TestConsumerCoordinatorStateIsPerClient(t *testing.T) {
	first := newCoordinatorMetricsConsumer(t, ConsumerClientRoleConsumer)
	// The metrics registry is process-wide, but coordinator routing state must not be.
	secondCfg := NewDefaultConsumerConfig()
	second, err := NewConsumer(secondCfg)
	require.NoError(t, err)
	t.Cleanup(second.rootCancel)

	first.setCoordinatorCache("coordinator-1:9000", coordinatorRouteCache, coordinatorReasonInitial)
	second.setCoordinatorCache("coordinator-2:9000", coordinatorRouteCache, coordinatorReasonInitial)
	assert.True(t, first.invalidateCoordinatorCacheIfCurrent("coordinator-1:9000", coordinatorReasonRedirect))

	first.mu.RLock()
	assert.Empty(t, first.coordinatorAddr)
	first.mu.RUnlock()
	second.mu.RLock()
	assert.Equal(t, "coordinator-2:9000", second.coordinatorAddr)
	second.mu.RUnlock()
}

func TestCoordinatorMetricLabelsAreBoundedAndExcludeSensitiveRoutingData(t *testing.T) {
	c := newCoordinatorMetricsConsumer(t, ConsumerClientRoleSaga)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	conn := c.trackCoordinatorConnection(client, "10.20.30.40:9000", coordinatorRouteCache)
	c.recordCoordinatorCommand(conn, "GROUP_STATUS group=private-group", coordinatorResultFailure)
	c.handleNotCoordinatorResponse(conn, "GROUP_STATUS", &BrokerError{
		Code:   "NOT_COORDINATOR",
		Fields: map[string]string{"host": "10.20.30.41", "port": "9000"},
	})

	families, err := metricsRegistry.Gather()
	require.NoError(t, err)
	allowedNames := map[string]bool{
		"command": true, "client_role": true, "route_source": true,
		"result": true, "reason": true, "le": true,
	}
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), "cursus_consumer_coordinator_") {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				assert.True(t, allowedNames[label.GetName()], "unexpected label %q on %s", label.GetName(), family.GetName())
				assert.NotContains(t, label.GetValue(), "private-group")
				assert.NotContains(t, label.GetValue(), "private-member")
				assert.NotContains(t, label.GetValue(), "10.20.30.")
				assert.NotContains(t, label.GetValue(), ":9000")
			}
		}
	}
}

func TestCoordinatorCommandLabelsCollapseUnknownCommands(t *testing.T) {
	assert.Equal(t, "heartbeat", coordinatorCommandLabel("HEARTBEAT group=workers"))
	assert.Equal(t, "group_status", coordinatorCommandLabel("group_status group=workers"))
	assert.Equal(t, "other", coordinatorCommandLabel("DYNAMIC_"+strings.Repeat("x", 32)))
}
