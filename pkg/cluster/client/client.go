package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
)

type TCPClusterClient struct {
	timeout time.Duration
}

func NewTCPClusterClient() *TCPClusterClient {
	return &TCPClusterClient{
		timeout: 5 * time.Second,
	}
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
		util.Warn("No seed hosts available for join; aborting join attempts")
		return fmt.Errorf("no seed hosts available")
	}

	maxAttempts := 8
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.attemptJoin(ctx, seedHosts, nodeID, addr, apiPort, attempt, maxAttempts); err != nil {
			util.Warn("Join attempt %d failed: %v", attempt, err)
		} else {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}

	return fmt.Errorf("failed to join cluster after %d attempts", maxAttempts)
}

func (c *TCPClusterClient) attemptJoin(ctx context.Context, seedHosts []string, nodeID, addr string, apiPort, attempt, maxAttempts int) error {
	payload := map[string]string{
		"node_id": nodeID,
		"address": addr,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal join payload: %w", err)
	}
	joinCmd := fmt.Sprintf("JOIN_CLUSTER %s", string(body))

	for _, seed := range seedHosts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hostOnly := seed
		if strings.Contains(seed, ":") {
			if h, _, err := net.SplitHostPort(seed); err == nil {
				hostOnly = h
			}
		}
		addr := net.JoinHostPort(hostOnly, fmt.Sprintf("%d", apiPort))
		util.Info("attempting cluster join to %s (attempt %d/%d)", addr, attempt, maxAttempts)

		if err := c.sendJoinCommand(ctx, addr, joinCmd); err != nil {
			util.Warn("join request to %s failed: %v", addr, err)
			continue
		}

		return nil
	}

	return fmt.Errorf("all join attempts failed")
}

func (c *TCPClusterClient) sendJoinCommand(ctx context.Context, addr, joinCmd string) error {
	dialer := &net.Dialer{
		Timeout: c.timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := util.WriteWithLength(conn, util.EncodeMessage("cluster", joinCmd)); err != nil {
		return fmt.Errorf("send command failed: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response failed: %w", err)
	}

	var jr struct {
		Success bool   `json:"success"`
		Leader  string `json:"leader"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(resp, &jr); err != nil {
		return fmt.Errorf("invalid response: %w", err)
	}

	if jr.Success {
		util.Info("join succeeded via %s; leader=%s", addr, jr.Leader)
		return nil
	}

	if jr.Leader != "" {
		return c.retryWithLeader(ctx, jr.Leader, 8000, joinCmd)
	}

	return fmt.Errorf("join failed: %s", jr.Error)
}

func (c *TCPClusterClient) retryWithLeader(ctx context.Context, leader string, apiPort int, joinCmd string) error {
	leaderHost := leader
	if strings.Contains(leader, ":") {
		if h, _, err := net.SplitHostPort(leader); err == nil {
			leaderHost = h
		}
	}

	addr := net.JoinHostPort(leaderHost, fmt.Sprintf("%d", apiPort))
	util.Info("retrying join at leader %s", addr)

	return c.sendJoinCommand(ctx, addr, joinCmd)
}

func (c *TCPClusterClient) extractSeedHosts(peers []string, localAddr string) []string {
	seedHosts := make([]string, 0, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		var addrOnly string
		if strings.Contains(p, "@") {
			parts := strings.SplitN(p, "@", 2)
			if len(parts) == 2 {
				addrOnly = parts[1]
			} else {
				addrOnly = p
			}
		} else {
			addrOnly = p
		}

		if addrOnly != localAddr {
			seedHosts = append(seedHosts, addrOnly)
		}
	}
	return seedHosts
}
