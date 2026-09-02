package wire

import (
	"fmt"
	"net"
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
	receiveMu sync.Mutex
	stateMu   sync.Mutex
	activeID  uint64
	activeCmd Command
	received  uint32
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
	c.received = 0
	c.awaiting = true
	c.stateMu.Unlock()
	if responseSuppressed(payload) {
		c.finishPending()
	}
	return nil
}

func (c *ClientConn) Receive() ([]byte, error) {
	if c == nil || c.wire == nil {
		return nil, fmt.Errorf("wire v2 client connection is not initialized")
	}
	c.receiveMu.Lock()
	defer c.receiveMu.Unlock()

	c.stateMu.Lock()
	activeID, activeCommand, awaiting := c.activeID, c.activeCmd, c.awaiting
	c.stateMu.Unlock()
	if !awaiting {
		return nil, fmt.Errorf("wire v2 client has no pending request")
	}
	frame, err := c.wire.ReadFrame()
	if err != nil {
		c.finishPending()
		return nil, err
	}
	if frame.Kind != KindResponse && frame.Kind != KindStream {
		c.finishPending()
		return nil, fmt.Errorf("unexpected Wire v2 response kind %d", frame.Kind)
	}
	if activeID == 0 || frame.RequestID != activeID || frame.Command != activeCommand {
		c.finishPending()
		return nil, fmt.Errorf(
			"wire v2 response correlation mismatch: request=%d/%s response=%d/%s",
			activeID, activeCommand, frame.RequestID, frame.Command,
		)
	}
	terminal := c.recordResponse(frame.Status)
	if terminal {
		c.finishPending()
	}
	if frame.Status == StatusError {
		payload, err := DecodeError(frame.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode Wire v2 broker error: %w", err)
		}
		return nil, NewBrokerError(payload)
	}
	if frame.Status != StatusOK && frame.Status != StatusStreamEnd {
		if !terminal {
			c.finishPending()
		}
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

func (c *ClientConn) finishPending() {
	c.stateMu.Lock()
	if !c.awaiting {
		c.stateMu.Unlock()
		return
	}
	c.awaiting = false
	c.activeID = 0
	c.activeCmd = CommandUnknown
	c.received = 0
	c.stateMu.Unlock()
	c.roundTrip.Unlock()
}

func (c *ClientConn) recordResponse(status Status) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.received++
	if status == StatusError {
		return true
	}
	switch c.activeCmd {
	case CommandReadStream:
		return status == StatusStreamEnd || c.received >= 2
	case CommandStream:
		return status == StatusStreamEnd
	default:
		return true
	}
}

func requestFramePayload(payload []byte) (Command, []byte, error) {
	if IsBatch(payload) {
		if _, err := DecodeBatch(payload); err != nil {
			return CommandUnknown, nil, err
		}
		return CommandPublish, payload, nil
	}
	command, request, err := ParseCommandText(string(payload))
	if err != nil {
		return CommandUnknown, nil, err
	}
	if command == CommandPublish {
		if err := validateAcks(request.Fields["acks"]); err != nil {
			return CommandUnknown, nil, err
		}
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
		return err == nil && batch.Acks == "0"
	}
	command, request, err := ParseCommandText(string(payload))
	if err != nil || command != CommandPublish {
		return false
	}
	return request.Fields["acks"] == "0"
}
