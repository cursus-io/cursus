package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
)

type TCPClusterClient struct {
	timeout   time.Duration
	authToken string
	tlsConfig *tls.Config
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

func (c *TCPClusterClient) secureCommand(command string) string {
	if c.authToken == "" {
		return command
	}
	return "AUTH " + c.authToken + " " + command
}

func (c *TCPClusterClient) dialContext(ctx context.Context, address string) (net.Conn, error) {
	dialer := &net.Dialer{}
	if c.tlsConfig == nil {
		return dialer.DialContext(ctx, "tcp", address)
	}
	tlsDialer := &tls.Dialer{NetDialer: dialer, Config: c.tlsConfig.Clone()}
	return tlsDialer.DialContext(ctx, "tcp", address)
}

func (c *TCPClusterClient) StartHeartbeat(ctx context.Context, peers []string, nodeID, localAddr string, discoveryPort int) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// sendHeartbeat internal loop uses goroutines now
				_ = c.sendHeartbeat(ctx, peers, nodeID, localAddr, discoveryPort)
			}
		}
	}()
}

func (c *TCPClusterClient) sendHeartbeat(ctx context.Context, peers []string, nodeID, localAddr string, discoveryPort int) error {
	apiPort := discoveryPort
	if apiPort == 0 {
		apiPort = 8000
	}

	payload := map[string]string{"node_id": nodeID}
	body, _ := json.Marshal(payload)
	cmd := fmt.Sprintf("HEARTBEAT_CLUSTER %s", string(body))

	for _, peer := range peers {
		// Launch each heartbeat in a separate goroutine to avoid blocking on DNS/Connection
		go func(p string) {
			addrOnly := p
			if strings.Contains(p, "@") {
				addrOnly = strings.Split(p, "@")[1]
			}

			if addrOnly == localAddr {
				return
			}

			host := addrOnly
			if strings.Contains(addrOnly, ":") {
				var err error
				host, _, err = net.SplitHostPort(addrOnly)
				if err != nil {
					host = addrOnly
				}
			}

			target := net.JoinHostPort(host, fmt.Sprintf("%d", apiPort))

			// Use short timeout for heartbeat connection
			heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
			conn, err := c.dialContext(heartbeatCtx, target)
			cancel()
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			// Set a write deadline to prevent goroutine buildup on slow connections
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_ = util.WriteWithLength(conn, util.EncodeMessage("cluster", c.secureCommand(cmd)))
		}(peer)
	}
	return nil
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
	payload := map[string]string{
		"node_id": nodeID,
		"address": localAddr,
	}
	body, _ := json.Marshal(payload)
	joinCmd := fmt.Sprintf("JOIN_CLUSTER %s", string(body))

	conn, err := c.dialContext(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Set connection deadlines based on the context's remaining time
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}

	if err := util.WriteWithLength(conn, util.EncodeMessage("cluster", c.secureCommand(joinCmd))); err != nil {
		return err
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return err
	}

	var jr struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(resp, &jr); err != nil {
		return err
	}

	if !jr.Success {
		return fmt.Errorf("%s", jr.Error)
	}

	return nil
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
