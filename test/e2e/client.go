package e2e

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

// BrokerClient wraps low-level broker communication
type BrokerClient struct {
	addrs         []string
	conn          net.Conn
	wire          *wire.Connection
	mu            sync.Mutex
	requestMu     sync.Mutex
	nextRequestID uint64
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
		bc.wire = nil
	}

	var lastErr error
	for _, addr := range bc.addrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			connection, handshakeErr := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
			if handshakeErr != nil {
				_ = conn.Close()
				lastErr = handshakeErr
				continue
			}
			_ = conn.SetDeadline(time.Time{})
			bc.conn = conn
			bc.wire = connection
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
		bc.wire = nil
	}
	bc.closed = true
}

func (bc *BrokerClient) SendCommand(cmdTopic, cmdPayload string, readTimeout time.Duration) (string, error) {
	_ = cmdTopic
	bc.requestMu.Lock()
	defer bc.requestMu.Unlock()
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

		command, requestID, err := bc.writeWireCommand(cmdPayload)
		if err != nil {
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

		respBuf, err := bc.readWirePayload(command, requestID)
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
		_ = conn.SetReadDeadline(time.Time{})

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
		bc.wire = nil
	}
	bc.wire = nil
}

func (bc *BrokerClient) writeWireCommand(commandText string) (wire.Command, uint64, error) {
	command, request, err := wire.ParseCommandText(commandText)
	if err != nil {
		return wire.CommandUnknown, 0, err
	}
	payload, err := wire.EncodeCommandPayload(request)
	if err != nil {
		return wire.CommandUnknown, 0, err
	}
	bc.mu.Lock()
	connection := bc.wire
	bc.nextRequestID++
	requestID := bc.nextRequestID
	bc.mu.Unlock()
	if connection == nil {
		return wire.CommandUnknown, 0, fmt.Errorf("wire v2 connection is not available")
	}
	if err := connection.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: requestID, Payload: payload,
	}); err != nil {
		return wire.CommandUnknown, 0, err
	}
	return command, requestID, nil
}

func (bc *BrokerClient) readWirePayload(command wire.Command, requestID uint64) ([]byte, error) {
	bc.mu.Lock()
	connection := bc.wire
	bc.mu.Unlock()
	if connection == nil {
		return nil, fmt.Errorf("wire v2 connection is not available")
	}
	response, err := connection.ReadFrame()
	if err != nil {
		return nil, err
	}
	if response.Command != command || response.RequestID != requestID ||
		(response.Kind != wire.KindResponse && response.Kind != wire.KindStream) {
		return nil, fmt.Errorf(
			"wire v2 response correlation mismatch: request=%d/%s response=%d/%s",
			requestID, command, response.RequestID, response.Command,
		)
	}
	if response.Status == wire.StatusError {
		payload, err := wire.DecodeError(response.Payload)
		if err != nil {
			return nil, err
		}
		return []byte(renderWireError(payload)), nil
	}
	if response.Status != wire.StatusOK && response.Status != wire.StatusStreamEnd {
		return nil, fmt.Errorf("unexpected Wire v2 response status %d", response.Status)
	}
	return response.Payload, nil
}

func renderWireError(payload wire.ErrorPayload) string {
	parts := []string{
		"ERROR:", payload.Code, "class=" + payload.Class.String(),
		"retryable=" + strconv.FormatBool(payload.Retryable),
	}
	keys := make([]string, 0, len(payload.Fields))
	for key := range payload.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key+"="+payload.Fields[key])
	}
	return strings.Join(parts, " ")
}

func sendOneWayWireCommand(address, commandText string) error {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	connection, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	command, request, err := wire.ParseCommandText(commandText)
	if err != nil {
		return err
	}
	payload, err := wire.EncodeCommandPayload(request)
	if err != nil {
		return err
	}
	return connection.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: 1, Payload: payload,
	})
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
