package transport

import (
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/wire"
)

// Conn is the sole SDK network path. It retains net.Conn for deadline and
// address compatibility while all application I/O uses Send and Receive.
type Conn struct {
	net.Conn
	wire      *wire.Connection
	nextID    atomic.Uint64
	roundTrip sync.Mutex
	stateMu   sync.Mutex
	activeID  uint64
	activeCmd wire.Command
	awaiting  bool
}

func NewClient(conn net.Conn, compression string) (*Conn, error) {
	selected, err := wire.ParseCompression(compression)
	if err != nil {
		return nil, err
	}
	preferences := []wire.Compression{selected}
	if selected != wire.CompressionNone {
		preferences = append(preferences, wire.CompressionNone)
	}
	negotiated, err := wire.ClientHandshake(conn, preferences)
	if err != nil {
		return nil, err
	}
	return &Conn{Conn: conn, wire: negotiated}, nil
}

func (c *Conn) Send(payload []byte) error {
	if c == nil || c.wire == nil {
		return fmt.Errorf("SDK Wire v2 connection is not initialized")
	}
	command, framePayload, err := requestFramePayload(payload)
	if err != nil {
		return err
	}
	c.roundTrip.Lock()
	requestID := c.nextID.Add(1)
	if err := c.wire.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: requestID, Payload: framePayload,
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

func (c *Conn) Receive() ([]byte, error) {
	if c == nil || c.wire == nil {
		return nil, fmt.Errorf("SDK Wire v2 connection is not initialized")
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
	if frame.Kind != wire.KindResponse && frame.Kind != wire.KindStream {
		return nil, fmt.Errorf("unexpected Wire v2 response kind %d", frame.Kind)
	}
	if activeID == 0 || frame.RequestID != activeID || frame.Command != activeCommand {
		return nil, fmt.Errorf(
			"Wire v2 response correlation mismatch: request=%d/%s response=%d/%s",
			activeID, activeCommand, frame.RequestID, frame.Command,
		)
	}
	if frame.Status == wire.StatusError {
		payload, err := wire.DecodeError(frame.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode Wire v2 broker error: %w", err)
		}
		return []byte(renderBrokerError(payload)), nil
	}
	if frame.Status != wire.StatusOK && frame.Status != wire.StatusStreamEnd {
		return nil, fmt.Errorf("unexpected Wire v2 response status %d", frame.Status)
	}
	return frame.Payload, nil
}

func (c *Conn) clearPending() {
	c.stateMu.Lock()
	c.awaiting = false
	c.stateMu.Unlock()
}

func requestFramePayload(payload []byte) (wire.Command, []byte, error) {
	if wire.IsBatch(payload) {
		return wire.CommandPublish, payload, nil
	}
	commandText := legacyCommandText(payload)
	command, request, err := wire.ParseCommandText(string(commandText))
	if err != nil {
		return wire.CommandUnknown, nil, err
	}
	encoded, err := wire.EncodeCommandPayload(request)
	if err != nil {
		return wire.CommandUnknown, nil, err
	}
	return command, encoded, nil
}

func legacyCommandText(payload []byte) []byte {
	if len(payload) >= 2 {
		topicLength := int(binary.BigEndian.Uint16(payload[:2]))
		if topicLength <= len(payload)-2 {
			return payload[2+topicLength:]
		}
	}
	return payload
}

func responseSuppressed(payload []byte) bool {
	if wire.IsBatch(payload) {
		batch, err := wire.DecodeBatch(payload)
		return err == nil && (batch.Acks == "0" || batch.Acks == "none")
	}
	command, request, err := wire.ParseCommandText(string(legacyCommandText(payload)))
	if err != nil || command != wire.CommandPublish {
		return false
	}
	return request.Fields["acks"] == "0" || request.Fields["acks"] == "none"
}

func renderBrokerError(payload wire.ErrorPayload) string {
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
