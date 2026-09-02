package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
)

type TCPClusterClient struct {
	timeout   time.Duration
	authToken string
	tlsConfig *tls.Config
	nextID    uint64
}

func NewTCPClusterClient() *TCPClusterClient {
	return NewSecureTCPClusterClient("", nil)
}

func NewSecureTCPClusterClient(authToken string, tlsConfig *tls.Config) *TCPClusterClient {
	return &TCPClusterClient{
		timeout:   5 * time.Second,
		authToken: authToken,
		tlsConfig: tlsConfig,
	}
}

func (c *TCPClusterClient) dialContext(ctx context.Context, address string) (net.Conn, error) {
	dialer := &net.Dialer{}
	if c.tlsConfig == nil {
		return dialer.DialContext(ctx, "tcp", address)
	}
	tlsDialer := &tls.Dialer{NetDialer: dialer, Config: c.tlsConfig.Clone()}
	return tlsDialer.DialContext(ctx, "tcp", address)
}

func (c *TCPClusterClient) StartHeartbeat(
	ctx context.Context,
	peers []string,
	nodeID, localAddr string,
	discoveryPort int,
	proofProvider func() []fsm.ISRCatchupProof,
) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var proofs []fsm.ISRCatchupProof
				if proofProvider != nil {
					proofs = proofProvider()
				}
				// sendHeartbeat internal loop uses goroutines now
				_ = c.sendHeartbeat(ctx, peers, nodeID, localAddr, discoveryPort, proofs)
			}
		}
	}()
}

func (c *TCPClusterClient) sendHeartbeat(
	ctx context.Context,
	peers []string,
	nodeID, localAddr string,
	discoveryPort int,
	proofs []fsm.ISRCatchupProof,
) error {
	apiPort := discoveryPort
	if apiPort == 0 {
		apiPort = 8000
	}

	fields := map[string]string{"node_id": nodeID}
	if len(proofs) > 0 {
		body, err := json.Marshal(proofs)
		if err != nil {
			return fmt.Errorf("marshal heartbeat proofs: %w", err)
		}
		fields["catchup_proofs"] = string(body)
	}

	targets := make([]string, 0, len(peers)+1)
	targets = append(targets, peers...)
	targets = append(targets, localAddr)
	seen := make(map[string]struct{}, len(targets))
	for _, peer := range targets {
		target := heartbeatTarget(peer, apiPort)
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		// Launch each heartbeat in a separate goroutine to avoid blocking on DNS/Connection
		go func(target string) {
			// Use short timeout for heartbeat connection
			heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, _ = c.sendRequest(heartbeatCtx, target, wire.CommandHeartbeatCluster, fields)
			cancel()
		}(target)
	}
	return nil
}

func heartbeatTarget(peer string, discoveryPort int) string {
	address := peer
	if _, suffix, ok := strings.Cut(peer, "@"); ok {
		address = suffix
	}
	host := address
	if parsedHost, _, err := net.SplitHostPort(address); err == nil {
		host = parsedHost
	}
	return net.JoinHostPort(host, strconv.Itoa(discoveryPort))
}

func (c *TCPClusterClient) JoinCluster(peers []string, nodeID, addr string, discoveryPort int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return c.joinClusterWithContext(ctx, peers, nodeID, addr, discoveryPort)
}

func (c *TCPClusterClient) joinClusterWithContext(ctx context.Context, peers []string, nodeID, addr string, discoveryPort int) error {
	apiPort := discoveryPort
	if apiPort == 0 {
		apiPort = 8000
	}

	seedHosts := c.extractSeedHosts(peers, addr)
	if len(seedHosts) == 0 {
		return fmt.Errorf("no seed hosts available")
	}

	for attempt := 1; attempt <= 5; attempt++ {
		for _, seed := range seedHosts {
			hostOnly := seed
			if strings.Contains(seed, ":") {
				if h, _, err := net.SplitHostPort(seed); err == nil {
					hostOnly = h
				}
			}
			targetAddr := net.JoinHostPort(hostOnly, fmt.Sprintf("%d", apiPort))

			// A single unresponsive seed must not consume the complete cluster join
			// deadline. Keep each connection attempt bounded while retaining the
			// caller's cancellation and overall deadline as the outer limit.
			attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
			err := c.sendJoinCommand(attemptCtx, targetAddr, nodeID, addr)
			cancel()
			if err == nil {
				return nil
			}
		}
		// Respect context cancellation while retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("failed to join cluster after 5 attempts")
}

func (c *TCPClusterClient) sendJoinCommand(ctx context.Context, addr, nodeID, localAddr string) error {
	response, err := c.sendRequest(ctx, addr, wire.CommandJoinCluster, map[string]string{
		"node_id": nodeID,
		"address": localAddr,
	})
	if err != nil {
		return err
	}

	var jr struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(response, &jr); err != nil {
		return err
	}

	if !jr.Success {
		return fmt.Errorf("%s", jr.Error)
	}

	return nil
}

func (c *TCPClusterClient) sendRequest(
	ctx context.Context,
	address string,
	command wire.Command,
	fields map[string]string,
) ([]byte, error) {
	conn, err := c.dialContext(ctx, address)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
	}

	connection, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return nil, err
	}
	requestFields := make(map[string]string, len(fields))
	for key, value := range fields {
		requestFields[key] = value
	}
	if c.authToken != "" {
		requestFields["auth_token"] = c.authToken
	}
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: requestFields})
	if err != nil {
		return nil, err
	}
	requestID := atomic.AddUint64(&c.nextID, 1)
	if err := connection.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: requestID, Payload: payload,
	}); err != nil {
		return nil, err
	}
	response, err := connection.ReadFrame()
	if err != nil {
		return nil, err
	}
	if response.Kind != wire.KindResponse || response.Command != command || response.RequestID != requestID {
		return nil, fmt.Errorf(
			"cluster Wire v2 response correlation mismatch: request=%d/%s response=%d/%s",
			requestID, command, response.RequestID, response.Command,
		)
	}
	if response.Status == wire.StatusError {
		brokerError, decodeErr := wire.DecodeError(response.Payload)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode cluster Wire v2 error: %w", decodeErr)
		}
		return nil, fmt.Errorf("%s: %s", brokerError.Code, brokerError.Message)
	}
	if response.Status != wire.StatusOK {
		return nil, fmt.Errorf("unexpected cluster Wire v2 status %d", response.Status)
	}
	return response.Payload, nil
}

func (c *TCPClusterClient) extractSeedHosts(peers []string, localAddr string) []string {
	seedHosts := make([]string, 0, len(peers))
	for _, p := range peers {
		addrOnly := p
		if strings.Contains(p, "@") {
			addrOnly = strings.Split(p, "@")[1]
		}
		if addrOnly != localAddr {
			seedHosts = append(seedHosts, addrOnly)
		}
	}
	return seedHosts
}
