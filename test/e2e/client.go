package e2e

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/util"
)

// BrokerClient wraps low-level broker communication
type BrokerClient struct {
	addrs         []string
	conn          net.Conn
	mu            sync.Mutex
	closed        bool
	topic         string
	consumerGroup string
	memberID      string // consumerID + uuid
	generation    int
}

// ConsumerGroupStatus represents consumer group metadata
type ConsumerGroupStatus struct {
	GroupName      string       `json:"group_name"`
	TopicName      string       `json:"topic_name"`
	State          string       `json:"state"`
	MemberCount    int          `json:"member_count"`
	PartitionCount int          `json:"partition_count"`
	Members        []MemberInfo `json:"members"`
	LastRebalance  time.Time    `json:"last_rebalance"`
}

type MemberInfo struct {
	MemberID      string    `json:"member_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Assignments   []int     `json:"assignments"`
}

func NewBrokerClient(addrs []string) *BrokerClient {
	return &BrokerClient{
		addrs: addrs,
	}
}

// GetMemberID returns the consumer member ID
func (bc *BrokerClient) GetMemberID() string {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.memberID
}

func (bc *BrokerClient) SetMemberID(id string) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.memberID = id
}

// GetGeneration returns the consumer generation
func (bc *BrokerClient) GetGeneration() int {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.generation
}

func (bc *BrokerClient) GetSyncInfo() (string, int) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.memberID, bc.generation
}

func (bc *BrokerClient) connect() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn != nil && !bc.closed {
		return nil
	}

	if bc.conn != nil {
		_ = bc.conn.Close()
		bc.conn = nil
	}

	var lastErr error
	for _, addr := range bc.addrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			bc.conn = conn
			bc.closed = false
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("failed to connect to any broker in %v: %w", bc.addrs, lastErr)
}

func (bc *BrokerClient) rotateAddrs() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if len(bc.addrs) > 1 {
		first := bc.addrs[0]
		bc.addrs = append(bc.addrs[1:], first)
	}
}

func (bc *BrokerClient) Close() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn != nil {
		if err := bc.conn.Close(); err != nil {
			util.Debug("failed to close connection: %v", err)
		}
		bc.conn = nil
	}
	bc.closed = true
}

func (bc *BrokerClient) SendCommand(cmdTopic, cmdPayload string, readTimeout time.Duration) (string, error) {
	const maxRetries = 5
	var lastErr error

	for i := 0; i <= maxRetries; i++ {
		if err := bc.connect(); err != nil {
			lastErr = err
			bc.rotateAddrs()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		bc.mu.Lock()
		conn := bc.conn
		bc.mu.Unlock()

		if conn == nil {
			lastErr = fmt.Errorf("connection is nil")
			continue
		}

		cmdBytes := util.EncodeMessage(cmdTopic, cmdPayload)
		if err := util.WriteWithLength(conn, cmdBytes); err != nil {
			bc.closeInternal()
			lastErr = err
			bc.rotateAddrs()

			if isIdempotent(cmdPayload) {
				util.Debug("Write error on idempotent command, retrying: %v", err)
				continue
			}

			return "", fmt.Errorf("write failed (possible duplicate if retried): %w", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			bc.closeInternal()
			return "", err
		}

		respBuf, err := util.ReadWithLength(conn)
		if err != nil {
			bc.closeInternal()
			lastErr = err
			bc.rotateAddrs()

			if isIdempotent(cmdPayload) {
				util.Debug("Read error on idempotent command, retrying: %v", err)
				continue
			}

			return "", fmt.Errorf("write succeeded but read failed (possible duplicate if retried): %w", err)
		}

		respStr := strings.TrimSpace(string(respBuf))
		if redirectAddr, ok := parseNotCoordinator(respStr); ok {
			bc.closeInternal()
			bc.preferAddr(redirectAddr)
			lastErr = fmt.Errorf("broker error: %s", respStr)
			util.Debug("Coordinator moved to %s, retrying (%d/%d)", redirectAddr, i, maxRetries)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if strings.Contains(respStr, "NOT_AUTHORIZED_FOR_PARTITION") {
			bc.closeInternal()
			bc.rotateAddrs()
			lastErr = fmt.Errorf("broker error: %s", respStr)
			util.Debug("Not authorized for partition, rotating and retrying (%d/%d)", i, maxRetries)
			time.Sleep(1 * time.Second)
			continue
		}

		return respStr, nil
	}
	return "", fmt.Errorf("command failed after retries: %w", lastErr)
}

func parseNotCoordinator(resp string) (string, bool) {
	if !strings.Contains(resp, "NOT_COORDINATOR") {
		return "", false
	}
	host := ""
	port := ""
	for _, part := range strings.Fields(resp) {
		if strings.HasPrefix(part, "host=") {
			host = strings.TrimPrefix(part, "host=")
		} else if strings.HasPrefix(part, "port=") {
			port = strings.TrimPrefix(part, "port=")
		}
	}
	if host == "" || port == "" {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}

func (bc *BrokerClient) preferAddr(addr string) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	next := []string{addr}
	for _, existing := range bc.addrs {
		if existing != addr {
			next = append(next, existing)
		}
	}
	bc.addrs = next
}

func (bc *BrokerClient) closeInternal() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if bc.conn != nil {
		_ = bc.conn.Close()
		bc.conn = nil
	}
}

func isIdempotent(payload string) bool {
	p := strings.ToUpper(strings.TrimSpace(payload))
	return strings.HasPrefix(p, "DESCRIBE") ||
		strings.HasPrefix(p, "LIST") ||
		strings.HasPrefix(p, "FETCH_OFFSET") ||
		strings.HasPrefix(p, "GROUP_STATUS") ||
		strings.HasPrefix(p, "HELP") ||
		strings.HasPrefix(p, "CONSUME") ||
		strings.HasPrefix(p, "LIST_GROUPS")
}

// executeCommand is a simplified wrapper for commands expected to return only "OK" or "ERROR:".
func (bc *BrokerClient) executeCommand(topic, payload string) error {
	resp, err := bc.SendCommand(topic, payload, 2*time.Second)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}
	return nil
}
