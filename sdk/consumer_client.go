package sdk

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
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

	if err := negotiateConfiguredProtocol(conn, c.config.ProtocolVersion, c.config.ProtocolFeatures, c.config.RequireProtocolFeatures, c.config.ProtocolNegotiationTimeoutMS); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("protocol negotiation with %s failed: %w", addr, err)
	}
	return conn, nil
}

// ConnectToAddr opens a connection to a specific address (e.g., the coordinator).
func (c *ConsumerClient) ConnectToAddr(addr string) (net.Conn, error) {
	return c.Connect(addr)
}

// ConnectWithFailover tries the cached leader first, then each broker in order.
func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	addrs := c.config.BrokerAddrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	leaderAddr := ""
	staleness := c.config.LeaderStaleness
	if staleness <= 0 {
		staleness = 30 * time.Second
	}
	if info := c.leader.Load(); info != nil && info.addr != "" && time.Since(info.updated) < staleness {
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

// ListOffsets queries the broker for retained and readable offsets on a topic.
func (c *ConsumerClient) ListOffsets(topic string, partition ...int) ([]PartitionOffsetRange, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if len(partition) > 1 {
		return nil, fmt.Errorf("at most one partition can be requested")
	}

	conn, _, err := c.ConnectWithFailover()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	cmd := fmt.Sprintf("LIST_OFFSETS topic=%s", topic)
	if len(partition) == 1 {
		cmd = fmt.Sprintf("%s partition=%d", cmd, partition[0])
	}
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return nil, fmt.Errorf("send list offsets: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read list offsets: %w", err)
	}
	return parseListOffsetsResponse(strings.TrimSpace(string(resp)))
}

func parseListOffsetsResponse(resp string) ([]PartitionOffsetRange, error) {
	if strings.HasPrefix(resp, "ERROR:") {
		return nil, fmt.Errorf("list offsets broker error: %s", resp)
	}
	if !strings.HasPrefix(resp, "OK") {
		return nil, fmt.Errorf("unexpected list offsets response: %s", resp)
	}

	offsetsValue := parseOKFields(resp)["offsets"]
	if offsetsValue == "" {
		return nil, fmt.Errorf("missing offsets in response: %s", resp)
	}

	entries := strings.Split(offsetsValue, ",")
	ranges := make([]PartitionOffsetRange, 0, len(entries))
	for _, entry := range entries {
		r, err := parseListOffsetEntry(entry)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

func parseListOffsetEntry(entry string) (PartitionOffsetRange, error) {
	parts := strings.Split(entry, ":")
	if len(parts) != 5 || !strings.HasPrefix(parts[0], "P") {
		return PartitionOffsetRange{}, fmt.Errorf("invalid list offsets entry: %s", entry)
	}
	partition, err := strconv.Atoi(strings.TrimPrefix(parts[0], "P"))
	if err != nil {
		return PartitionOffsetRange{}, fmt.Errorf("invalid list offsets partition: %s", entry)
	}

	r := PartitionOffsetRange{Partition: partition}
	seen := map[string]bool{}
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return PartitionOffsetRange{}, fmt.Errorf("invalid list offsets field: %s", part)
		}
		value, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			return PartitionOffsetRange{}, fmt.Errorf("invalid list offsets value: %s", part)
		}
		switch kv[0] {
		case "earliest":
			r.Earliest = value
		case "latest":
			r.Latest = value
		case "leo":
			r.LEO = value
		case "hwm":
			r.HWM = value
		default:
			return PartitionOffsetRange{}, fmt.Errorf("unknown list offsets field: %s", kv[0])
		}
		seen[kv[0]] = true
	}
	for _, key := range []string{"earliest", "latest", "leo", "hwm"} {
		if !seen[key] {
			return PartitionOffsetRange{}, fmt.Errorf("missing list offsets field %s: %s", key, entry)
		}
	}
	return r, nil
}
