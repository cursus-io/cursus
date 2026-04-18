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
				_ = c.sendHeartbeat(peers, nodeID, localAddr, discoveryPort)
			}
		}
	}()
}

func (c *TCPClusterClient) sendHeartbeat(peers []string, nodeID, localAddr string, discoveryPort int) error {
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
			conn, err := net.DialTimeout("tcp", target, 1*time.Second)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			// Set a write deadline to prevent goroutine buildup on slow connections
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_ = util.WriteWithLength(conn, util.EncodeMessage("cluster", cmd))
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

			if err := c.sendJoinCommand(ctx, targetAddr, nodeID, addr); err == nil {
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

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
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

	if err := util.WriteWithLength(conn, util.EncodeMessage("cluster", joinCmd)); err != nil {
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
