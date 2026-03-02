package client

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
	"github.com/google/uuid"

	"github.com/cursus-io/cursus/test/consumer/config"
)

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ConsumerClient struct {
	ID     string
	config *config.ConsumerConfig
	leader atomic.Value
}

func NewConsumerClient(cfg *config.ConsumerConfig) *ConsumerClient {
	c := &ConsumerClient{
		ID:     uuid.New().String(),
		config: cfg,
	}
	c.leader.Store(&leaderInfo{addr: "", updated: time.Time{}})
	return c
}

func (c *ConsumerClient) UpdateLeader(addr string) {
	oldInfo := c.leader.Load().(*leaderInfo)
	if oldInfo.addr != addr {
		c.leader.Store(&leaderInfo{
			addr:    addr,
			updated: time.Now(),
		})
		util.Info("📍 Leader updated to: %s (Lock-free update)", addr)
	}
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			util.Error("failed to set no delay: %v", err)
		}
		if err := tcpConn.SetKeepAlive(true); err != nil {
			util.Error("failed to set keep alive: %v", err)
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			util.Error("failed to set keep alive period: %v", err)
		}
		if err := tcpConn.SetReadBuffer(2 * 1024 * 1024); err != nil { // 2MB
			util.Error("failed to set read buffer: %v", err)
		}
		if err := tcpConn.SetWriteBuffer(2 * 1024 * 1024); err != nil { // 2MB
			util.Error("failed to set write buffer: %v", err)
		}
	}

	return conn, nil
}

func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	addrs := c.config.BrokerAddrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	leaderAddr := ""
	raw := c.leader.Load()
	if info, ok := raw.(*leaderInfo); ok && info.addr != "" && time.Since(info.updated) < c.config.LeaderStaleness {
		leaderAddr = info.addr
	}

	if leaderAddr != "" {
		if conn, err := c.Connect(leaderAddr); err == nil {
			return conn, leaderAddr, nil
		}
		util.Warn("Leader %s failed, falling back to other brokers", leaderAddr)
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
		util.Warn("Failover: failed to connect to %s: %v", addr, err)
	}

	if lastErr != nil {
		return nil, "", fmt.Errorf("all brokers unreachable: %w", lastErr)
	}
	return nil, "", fmt.Errorf("all brokers unreachable")
}
