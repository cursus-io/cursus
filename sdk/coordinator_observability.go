package sdk

import (
	"net"
	"strings"
	"time"
)

const (
	coordinatorResultSuccess = "success"
	coordinatorResultFailure = "failure"

	coordinatorReasonInitial   = "initial"
	coordinatorReasonRedirect  = "redirect"
	coordinatorReasonReconnect = "reconnect"
	coordinatorReasonCacheMiss = "cache_miss"

	coordinatorRouteBootstrap = "bootstrap"
	coordinatorRouteCache     = "cache"
	coordinatorRouteRetry     = "retry"
	coordinatorRouteReconnect = "reconnect"

	coordinatorLogInterval = 30 * time.Second
)

var coordinatorCommandLabels = map[string]string{
	"JOIN_GROUP":    "join_group",
	"SYNC_GROUP":    "sync_group",
	"LEAVE_GROUP":   "leave_group",
	"HEARTBEAT":     "heartbeat",
	"FETCH_OFFSET":  "fetch_offset",
	"COMMIT_OFFSET": "commit_offset",
	"BATCH_COMMIT":  "batch_commit",
	"GROUP_STATUS":  "group_status",
}

type coordinatorConnectionMetadata struct {
	address     string
	routeSource string
}

func coordinatorCommandLabel(command string) string {
	name := strings.ToUpper(strings.TrimSpace(command))
	if i := strings.IndexByte(name, ' '); i >= 0 {
		name = name[:i]
	}
	if label, ok := coordinatorCommandLabels[name]; ok {
		return label
	}
	return "other"
}

func normalizeCoordinatorReason(reason string) string {
	switch reason {
	case coordinatorReasonInitial, coordinatorReasonRedirect, coordinatorReasonReconnect, coordinatorReasonCacheMiss:
		return reason
	default:
		return coordinatorReasonCacheMiss
	}
}

func normalizeCoordinatorRoute(source string) string {
	switch source {
	case coordinatorRouteBootstrap, coordinatorRouteCache, coordinatorRouteRetry, coordinatorRouteReconnect:
		return source
	default:
		return coordinatorRouteBootstrap
	}
}

func (c *Consumer) coordinatorClientRole() string {
	if c == nil || c.config == nil || !validConsumerClientRole(c.config.ClientRole) {
		return string(ConsumerClientRoleConsumer)
	}
	return string(c.config.ClientRole)
}

func (c *Consumer) coordinatorMetricsEnabled() bool {
	return c != nil && c.config != nil && c.config.EnableMetrics
}

func (c *Consumer) recordCoordinatorResolution(result, reason string, elapsed time.Duration) {
	if !c.coordinatorMetricsEnabled() {
		return
	}
	initMetrics()
	reason = normalizeCoordinatorReason(reason)
	role := c.coordinatorClientRole()
	consumerCoordinatorResolution.WithLabelValues(result, reason, role).Inc()
	consumerCoordinatorResolutionLag.WithLabelValues(result, reason, role).Observe(elapsed.Seconds())
}

func (c *Consumer) recordCoordinatorCacheRefresh(result, reason string) {
	if !c.coordinatorMetricsEnabled() {
		return
	}
	initMetrics()
	consumerCoordinatorCacheRefresh.WithLabelValues(result, normalizeCoordinatorReason(reason), c.coordinatorClientRole()).Inc()
}

func (c *Consumer) recordCoordinatorCommand(conn net.Conn, command, result string) {
	if !c.coordinatorMetricsEnabled() {
		if result == coordinatorResultSuccess {
			c.promoteCoordinatorRoute(conn)
		}
		return
	}
	initMetrics()
	label := coordinatorCommandLabel(command)
	source := c.coordinatorRouteOf(conn)
	role := c.coordinatorClientRole()
	consumerCoordinatorCommands.WithLabelValues(label, role, source, result).Inc()
	if source == coordinatorRouteRetry {
		consumerCoordinatorRetry.WithLabelValues(label, result, role).Inc()
	}
	if result == coordinatorResultSuccess {
		c.promoteCoordinatorRoute(conn)
	}
}

func (c *Consumer) recordCoordinatorRedirect(conn net.Conn, command string) {
	if !c.coordinatorMetricsEnabled() {
		return
	}
	initMetrics()
	consumerCoordinatorRedirects.WithLabelValues(
		coordinatorCommandLabel(command),
		c.coordinatorClientRole(),
		c.coordinatorRouteOf(conn),
	).Inc()
}

func (c *Consumer) coordinatorRouteOf(conn net.Conn) string {
	if c != nil && conn != nil {
		c.coordinatorConnMu.RLock()
		tracked, ok := c.coordinatorConns[conn]
		c.coordinatorConnMu.RUnlock()
		if ok {
			return normalizeCoordinatorRoute(tracked.routeSource)
		}
	}
	return coordinatorRouteBootstrap
}

func (c *Consumer) coordinatorAddressOf(conn net.Conn) string {
	if c != nil && conn != nil {
		c.coordinatorConnMu.RLock()
		tracked, ok := c.coordinatorConns[conn]
		c.coordinatorConnMu.RUnlock()
		if ok {
			return tracked.address
		}
	}
	if conn != nil && conn.RemoteAddr() != nil {
		return conn.RemoteAddr().String()
	}
	return ""
}

func (c *Consumer) trackCoordinatorConnection(conn net.Conn, address, routeSource string) net.Conn {
	if conn == nil {
		return nil
	}
	c.coordinatorConnMu.Lock()
	if c.coordinatorConns == nil {
		c.coordinatorConns = make(map[net.Conn]coordinatorConnectionMetadata)
	}
	c.coordinatorConns[conn] = coordinatorConnectionMetadata{address: address, routeSource: normalizeCoordinatorRoute(routeSource)}
	c.coordinatorConnMu.Unlock()
	return conn
}

func (c *Consumer) forgetCoordinatorConnection(conn net.Conn) {
	if c == nil || conn == nil {
		return
	}
	c.coordinatorConnMu.Lock()
	delete(c.coordinatorConns, conn)
	c.coordinatorConnMu.Unlock()
}

func (c *Consumer) coordinatorCacheSnapshot() (address, source, pendingReason string, reconnect bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	address = c.coordinatorAddr
	source = normalizeCoordinatorRoute(c.coordinatorRouteSource)
	pendingReason = normalizeCoordinatorReason(c.coordinatorPendingReason)
	reconnect = c.coordinatorReconnectPending
	c.coordinatorReconnectPending = false
	return
}

func (c *Consumer) setCoordinatorCache(address, source, reason string) {
	address = strings.TrimSpace(address)
	if address == "" {
		c.recordCoordinatorCacheRefresh(coordinatorResultFailure, reason)
		return
	}
	c.mu.Lock()
	old := c.coordinatorAddr
	c.coordinatorAddr = address
	c.coordinatorRouteSource = normalizeCoordinatorRoute(source)
	c.coordinatorPendingReason = ""
	c.mu.Unlock()
	c.recordCoordinatorCacheRefresh(coordinatorResultSuccess, reason)
	if reason == coordinatorReasonRedirect || reason == coordinatorReasonReconnect {
		c.logCoordinatorEvent("coordinator_cache_refresh", "", source, reason, coordinatorResultSuccess, old, address, false, 0, 0)
	}
}

func (c *Consumer) setCoordinatorCacheIfCurrent(expected, address, source, reason string) (string, string, bool) {
	address = strings.TrimSpace(address)
	if address == "" {
		c.recordCoordinatorCacheRefresh(coordinatorResultFailure, reason)
		return "", coordinatorRouteBootstrap, false
	}
	c.mu.Lock()
	if c.coordinatorAddr != expected {
		currentAddress := c.coordinatorAddr
		currentSource := normalizeCoordinatorRoute(c.coordinatorRouteSource)
		c.mu.Unlock()
		return currentAddress, currentSource, false
	}
	old := c.coordinatorAddr
	c.coordinatorAddr = address
	c.coordinatorRouteSource = normalizeCoordinatorRoute(source)
	c.coordinatorPendingReason = ""
	c.mu.Unlock()
	c.recordCoordinatorCacheRefresh(coordinatorResultSuccess, reason)
	if reason == coordinatorReasonRedirect || reason == coordinatorReasonReconnect {
		c.logCoordinatorEvent("coordinator_cache_refresh", "", source, reason, coordinatorResultSuccess, old, address, false, 0, 0)
	}
	return address, normalizeCoordinatorRoute(source), true
}

func (c *Consumer) invalidateCoordinatorCacheIfCurrent(expected, reason string) bool {
	c.mu.Lock()
	if c.coordinatorAddr != expected {
		c.mu.Unlock()
		return false
	}
	c.coordinatorAddr = ""
	c.coordinatorRouteSource = coordinatorRouteBootstrap
	c.coordinatorPendingReason = normalizeCoordinatorReason(reason)
	c.mu.Unlock()
	return true
}

func (c *Consumer) markCoordinatorReconnect() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.coordinatorReconnectPending = true
	c.mu.Unlock()
}

func (c *Consumer) promoteCoordinatorRoute(conn net.Conn) {
	c.mu.Lock()
	c.coordinatorConnMu.Lock()
	tracked, ok := c.coordinatorConns[conn]
	if ok &&
		(tracked.routeSource == coordinatorRouteRetry || tracked.routeSource == coordinatorRouteReconnect) &&
		c.coordinatorAddr == tracked.address {
		c.coordinatorRouteSource = coordinatorRouteCache
		tracked.routeSource = coordinatorRouteCache
		c.coordinatorConns[conn] = tracked
	}
	c.coordinatorConnMu.Unlock()
	c.mu.Unlock()
}

func (c *Consumer) handleNotCoordinatorResponse(conn net.Conn, command string, brokerErr *BrokerError) bool {
	if brokerErr == nil || !strings.EqualFold(brokerErr.Code, "NOT_COORDINATOR") {
		return false
	}
	c.recordCoordinatorRedirect(conn, command)

	oldAddress := c.coordinatorAddressOf(conn)
	if oldAddress == "" {
		c.mu.RLock()
		oldAddress = c.coordinatorAddr
		c.mu.RUnlock()
	}
	host, port := brokerErr.Fields["host"], brokerErr.Fields["port"]
	newAddress := ""
	if host != "" && port != "" {
		newAddress = c.coordinatorAddrFromHostPort(host, port)
	}
	sameCoordinator := newAddress != "" && newAddress == oldAddress
	configuredPin := ""
	if c.config != nil {
		configuredPin = strings.TrimSpace(c.config.CoordinatorAddr)
	}

	cacheRefreshed := false
	c.mu.Lock()
	responseFromSupersededRoute := c.coordinatorAddr != "" && oldAddress != "" && c.coordinatorAddr != oldAddress
	if !responseFromSupersededRoute {
		pinIsCurrent := newAddress == "" && configuredPin != "" && oldAddress == configuredPin && c.coordinatorAddr == configuredPin
		if pinIsCurrent {
			c.coordinatorRouteSource = coordinatorRouteRetry
			c.coordinatorPendingReason = ""
		} else if newAddress == "" || sameCoordinator {
			c.coordinatorAddr = ""
			c.coordinatorRouteSource = coordinatorRouteBootstrap
			c.coordinatorPendingReason = coordinatorReasonRedirect
		} else {
			c.coordinatorAddr = newAddress
			c.coordinatorRouteSource = coordinatorRouteRetry
			c.coordinatorPendingReason = ""
			cacheRefreshed = true
		}
	}
	c.mu.Unlock()
	if cacheRefreshed {
		c.recordCoordinatorCacheRefresh(coordinatorResultSuccess, coordinatorReasonRedirect)
		c.logCoordinatorEvent("coordinator_cache_refresh", "", coordinatorRouteRetry, coordinatorReasonRedirect, coordinatorResultSuccess, oldAddress, newAddress, false, 0, 0)
	}
	c.logCoordinatorEvent("coordinator_redirect", command, c.coordinatorRouteOf(conn), coordinatorReasonRedirect, coordinatorResultFailure, oldAddress, newAddress, sameCoordinator, 0, 0)
	return true
}

func (c *Consumer) logCoordinatorRetryExhausted(command string, attempt, limit int) {
	c.logCoordinatorEvent("coordinator_retry_exhausted", command, coordinatorRouteRetry, coordinatorReasonRedirect, coordinatorResultFailure, "", "", false, attempt, limit)
}

func (c *Consumer) logCoordinatorEvent(event, command, routeSource, reason, result, oldAddress, newAddress string, sameCoordinator bool, attempt, limit int) {
	if c == nil || c.config == nil {
		return
	}
	key := strings.Join([]string{event, coordinatorCommandLabel(command), normalizeCoordinatorRoute(routeSource), normalizeCoordinatorReason(reason), result}, "|")
	now := time.Now()
	c.coordinatorLogMu.Lock()
	if c.coordinatorLogLast == nil {
		c.coordinatorLogLast = make(map[string]time.Time)
	}
	if previous := c.coordinatorLogLast[key]; !previous.IsZero() && now.Sub(previous) < coordinatorLogInterval {
		c.coordinatorLogMu.Unlock()
		return
	}
	c.coordinatorLogLast[key] = now
	c.coordinatorLogMu.Unlock()

	c.mu.RLock()
	member := c.memberID
	c.mu.RUnlock()
	LogWarn(
		"event=%s command=%s client_role=%s route_source=%s reason=%s result=%s group=%q topic=%q member=%q old_coordinator=%q new_coordinator=%q same_coordinator=%t retry_attempt=%d retry_limit=%d",
		event,
		coordinatorCommandLabel(command),
		c.coordinatorClientRole(),
		normalizeCoordinatorRoute(routeSource),
		normalizeCoordinatorReason(reason),
		result,
		c.config.GroupID,
		c.config.Topic,
		member,
		oldAddress,
		newAddress,
		sameCoordinator,
		attempt,
		limit,
	)
}
