package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

var brokerCompressions = []wire.Compression{
	wire.CompressionNone,
	wire.CompressionGZIP,
	wire.CompressionSnappy,
	wire.CompressionLZ4,
}

// serverWireConn adapts the remaining controller net.Conn writers to Wire v2
// frames while those packages are split into typed response encoders.
type serverWireConn struct {
	net.Conn
	connection *wire.Connection

	mu       sync.Mutex
	buffer   []byte
	expected int
	request  wire.Frame
}

func newServerWireConn(conn net.Conn, connection *wire.Connection) *serverWireConn {
	return &serverWireConn{Conn: conn, connection: connection, expected: -1}
}

func (c *serverWireConn) setRequest(request wire.Frame) {
	c.mu.Lock()
	c.request = request
	c.mu.Unlock()
}

func (c *serverWireConn) Write(payload []byte) (int, error) {
	if c == nil || c.connection == nil {
		return 0, fmt.Errorf("server Wire v2 connection is not initialized")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buffer = append(c.buffer, payload...)
	for {
		if c.expected < 0 {
			if len(c.buffer) < 4 {
				break
			}
			length := binary.BigEndian.Uint32(c.buffer[:4])
			if length > wire.MaxFramePayload {
				return 0, fmt.Errorf("response payload size %d exceeds maximum %d", length, wire.MaxFramePayload)
			}
			c.expected = int(length)
			c.buffer = c.buffer[4:]
		}
		if len(c.buffer) < c.expected {
			break
		}
		message := append([]byte(nil), c.buffer[:c.expected]...)
		c.buffer = c.buffer[c.expected:]
		c.expected = -1
		if err := c.writeMessage(message); err != nil {
			return 0, err
		}
	}
	return len(payload), nil
}

func (c *serverWireConn) writeMessage(payload []byte) error {
	status := wire.StatusOK
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(string(payload))), "ERROR:") {
		status = wire.StatusError
		parsed, ok := wireprotocol.ParseErrorResponse(string(payload))
		if !ok {
			return fmt.Errorf("invalid broker error response")
		}
		class, err := wire.ParseErrorClass(string(parsed.Class))
		if err != nil {
			return err
		}
		fields := make(map[string]string, len(parsed.Fields))
		for key, value := range parsed.Fields {
			if key != "class" && key != "retryable" {
				fields[key] = value
			}
		}
		payload, err = wire.EncodeError(wire.ErrorPayload{
			Code: parsed.Code, Class: class, Retryable: parsed.Retryable,
			Message: strings.Join(parsed.Details, " "), Fields: fields,
		})
		if err != nil {
			return err
		}
	}
	kind := wire.KindResponse
	if c.request.Command == wire.CommandStream {
		kind = wire.KindStream
		if strings.HasPrefix(string(payload), "STREAM_CONTROL type=close") {
			status = wire.StatusStreamEnd
		}
	}
	return c.connection.WriteFrame(wire.Frame{
		Kind: kind, Command: c.request.Command, Status: status, RequestID: c.request.RequestID, Payload: payload,
	})
}

func negotiateServerConnection(conn net.Conn) (*wire.Connection, *serverWireConn, error) {
	connection, err := wire.ServerHandshake(conn, brokerCompressions)
	if err != nil {
		return nil, nil, err
	}
	return connection, newServerWireConn(conn, connection), nil
}

func readWireRequest(connection *wire.Connection) (wire.Frame, error) {
	frame, err := connection.ReadFrame()
	if err != nil {
		return wire.Frame{}, err
	}
	if frame.Kind != wire.KindRequest || frame.Status != wire.StatusNone || frame.RequestID == 0 {
		return wire.Frame{}, fmt.Errorf("invalid Wire v2 request frame")
	}
	if wire.IsBatch(frame.Payload) {
		if frame.Command != wire.CommandPublish {
			return wire.Frame{}, fmt.Errorf("Wire v2 batch requires PUBLISH command")
		}
		return frame, nil
	}
	payload, err := wire.DecodeCommandPayload(frame.Payload)
	if err != nil {
		return wire.Frame{}, fmt.Errorf("decode %s request: %w", frame.Command, err)
	}
	command, err := wire.RenderCommand(frame.Command, payload)
	if err != nil {
		return wire.Frame{}, err
	}
	frame.Payload = []byte(command)
	return frame, nil
}

func (c *serverWireConn) SetDeadline(deadline time.Time) error {
	return c.Conn.SetDeadline(deadline)
}
