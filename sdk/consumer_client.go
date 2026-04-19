package sdk

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type consumerLeaderInfo struct {
	addr    string
	updated time.Time
}

// ConsumerClient manages broker connections with leader-aware failover.
type ConsumerClient struct {
	ID        string
	config    *ConsumerConfig
	leader    atomic.Pointer[consumerLeaderInfo]
	tlsConfig *tls.Config
}

func NewConsumerClient(cfg *ConsumerConfig) (*ConsumerClient, error) {
	c := &ConsumerClient{
		ID:     uuid.New().String(),
		config: cfg,
	}
	c.leader.Store(&consumerLeaderInfo{addr: "", updated: time.Time{}})

	if cfg.UseTLS {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		c.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	return c, nil
}

func (c *ConsumerClient) UpdateLeader(addr string) {
	oldInfo := c.leader.Load()
	if oldInfo.addr != addr {
		c.leader.Store(&consumerLeaderInfo{
			addr:    addr,
			updated: time.Now(),
		})
		LogDebug("Updated leader: %s", addr)
	}
}

// Connect opens a TCP (or TLS) connection to addr with socket tuning applied.
func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	var conn net.Conn
	var err error

	if c.config.UseTLS {
		if c.tlsConfig == nil {
			return nil, fmt.Errorf("TLS enabled but certificate not loaded")
		}
		conn, err = tls.DialWithDialer(
			&net.Dialer{Timeout: 5 * time.Second},
			"tcp", addr, c.tlsConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("TLS dial to %s failed: %w", addr, err)
		}
	} else {
		dialer := net.Dialer{Timeout: 5 * time.Second}
		conn, err = dialer.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("dial failed to %s: %w", addr, err)
		}
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		_ = tcpConn.SetReadBuffer(2 * 1024 * 1024)
		_ = tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}

	return conn, nil
}

// ConnectWithFailover tries the cached leader first, then each broker in order.
func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	addrs := c.config.BrokerAddrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	leaderAddr := ""
	if info := c.leader.Load(); info != nil && info.addr != "" && time.Since(info.updated) < c.config.LeaderStaleness {
		leaderAddr = info.addr
	}

	if leaderAddr != "" {
		conn, err := c.Connect(leaderAddr)
		if err == nil {
			return conn, leaderAddr, nil
		}
		LogWarn("Cached leader %s unreachable: %v", leaderAddr, err)
	}

	var lastErr error
	for _, addr := range addrs {
		if addr == leaderAddr {
			continue
		}
		conn, err := c.Connect(addr)
		if err == nil {
			c.UpdateLeader(addr)
			return conn, addr, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return nil, "", fmt.Errorf("all brokers unreachable: %w", lastErr)
	}
	return nil, "", fmt.Errorf("all brokers unreachable")
}
