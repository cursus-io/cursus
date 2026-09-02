package wire

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ClientConn is the canonical Wire v2 client connection. It retains net.Conn
// for deadlines and addresses while serializing correlated request lifecycles.
type ClientConn struct {
	net.Conn
	wire      *Connection
	nextID    atomic.Uint64
	roundTrip sync.Mutex
	stateMu   sync.Mutex
	activeID  uint64
	activeCmd Command
	awaiting  bool
}

func NewClientConn(conn net.Conn, compression string) (*ClientConn, error) {
	selected, err := ParseCompression(compression)
	if err != nil {
		return nil, err
	}
	preferences := []Compression{selected}
	if selected != CompressionNone {
		preferences = append(preferences, CompressionNone)
	}
	negotiated, err := ClientHandshake(conn, preferences)
	if err != nil {
		return nil, err
	}
	return &ClientConn{Conn: conn, wire: negotiated}, nil
}

func (c *ClientConn) Send(payload []byte) error {
	if c == nil || c.wire == nil {
		return fmt.Errorf("wire v2 client connection is not initialized")
	}
	command, framePayload, err := requestFramePayload(payload)
	if err != nil {
		return err
	}
	c.roundTrip.Lock()
	requestID := c.nextID.Add(1)
	if err := c.wire.WriteFrame(Frame{
		Kind: KindRequest, Command: command, RequestID: requestID, Payload: framePayload,
	}); err != nil {
		c.roundTrip.Unlock()
		return err
	}
	c.stateMu.Lock()
	c.activeID = requestID
	c.activeCmd = command
	c.awaiting = true
	c.stateMu.Unlock()
	if responseSuppressed(payload) {
		c.clearPending()
		c.roundTrip.Unlock()
	}
	return nil
}

func (c *ClientConn) Receive() ([]byte, error) {
	if c == nil || c.wire == nil {
		return nil, fmt.Errorf("wire v2 client connection is not initialized")
	}
	c.stateMu.Lock()
	activeID, activeCommand, awaiting := c.activeID, c.activeCmd, c.awaiting
	c.stateMu.Unlock()
	frame, err := c.wire.ReadFrame()
	if awaiting {
		c.clearPending()
		c.roundTrip.Unlock()
	}
	if err != nil {
		return nil, err
	}
	if frame.Kind != KindResponse && frame.Kind != KindStream {
		return nil, fmt.Errorf("unexpected Wire v2 response kind %d", frame.Kind)
	}
	if activeID == 0 || frame.RequestID != activeID || frame.Command != activeCommand {
		return nil, fmt.Errorf(
			"wire v2 response correlation mismatch: request=%d/%s response=%d/%s",
			activeID, activeCommand, frame.RequestID, frame.Command,
		)
	}
	if frame.Status == StatusError {
		payload, err := DecodeError(frame.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode Wire v2 broker error: %w", err)
		}
		return []byte(renderBrokerError(payload)), nil
	}
	if frame.Status != StatusOK && frame.Status != StatusStreamEnd {
		return nil, fmt.Errorf("unexpected Wire v2 response status %d", frame.Status)
	}
	return frame.Payload, nil
}

func (c *ClientConn) WritePayload(payload []byte) error {
	return c.Send(payload)
}

func (c *ClientConn) ReadPayload() ([]byte, error) {
	return c.Receive()
}

func (c *ClientConn) clearPending() {
	c.stateMu.Lock()
	c.awaiting = false
	c.stateMu.Unlock()
}

func requestFramePayload(payload []byte) (Command, []byte, error) {
	if IsBatch(payload) {
		return CommandPublish, payload, nil
	}
	command, request, err := ParseCommandText(string(payload))
	if err != nil {
		return CommandUnknown, nil, err
	}
	encoded, err := EncodeCommandPayload(request)
	if err != nil {
		return CommandUnknown, nil, err
	}
	return command, encoded, nil
}

func responseSuppressed(payload []byte) bool {
	if IsBatch(payload) {
		batch, err := DecodeBatch(payload)
		return err == nil && (batch.Acks == "0" || batch.Acks == "none")
	}
	command, request, err := ParseCommandText(string(payload))
	if err != nil || command != CommandPublish {
		return false
	}
	return request.Fields["acks"] == "0" || request.Fields["acks"] == "none"
}

func renderBrokerError(payload ErrorPayload) string {
	parts := []string{
		"ERROR:", payload.Code,
		"class=" + payload.Class.String(),
		"retryable=" + strconv.FormatBool(payload.Retryable),
	}
	if payload.Message != "" {
		parts = append(parts, "message="+quoteErrorField(payload.Message))
	}
	keys := make([]string, 0, len(payload.Fields))
	for key := range payload.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key+"="+quoteErrorField(payload.Fields[key]))
	}
	return strings.Join(parts, " ")
}

func quoteErrorField(value string) string {
	if strings.ContainsAny(value, " \t\r\n\"") {
		return strconv.Quote(value)
	}
	return value
}
