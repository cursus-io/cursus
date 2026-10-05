package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

var brokerCompressions = []wire.Compression{
	wire.CompressionNone,
	wire.CompressionGZIP,
	wire.CompressionSnappy,
	wire.CompressionLZ4,
}

// serverWireConn exposes connection lifecycle operations to handlers and sends
// every handler payload as a correlated Wire v2 response or stream frame.
type serverWireConn struct {
	net.Conn
	connection *wire.Connection

	mu              sync.Mutex
	request         wire.Frame
	requestDeadline time.Time
	writeTimeout    time.Duration
}

func newServerWireConn(conn net.Conn, connection *wire.Connection, writeTimeout time.Duration) *serverWireConn {
	if writeTimeout <= 0 {
		writeTimeout = 30 * time.Second
	}
	return &serverWireConn{Conn: conn, connection: connection, writeTimeout: writeTimeout}
}

func (c *serverWireConn) setRequest(request wire.Frame, requestCtx context.Context) {
	// Responses need only correlation metadata. Retaining the payload here would
	// keep a completed request alive after its admission reservation is released.
	request.Payload = nil
	c.mu.Lock()
	c.request = request
	c.requestDeadline = time.Time{}
	if requestCtx != nil {
		c.requestDeadline, _ = requestCtx.Deadline()
	}
	c.mu.Unlock()
}

func (c *serverWireConn) WritePayload(payload []byte) error {
	if c == nil || c.connection == nil {
		return fmt.Errorf("server Wire v2 connection is not initialized")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeMessage(payload)
}

func (c *serverWireConn) writeMessage(payload []byte) error {
	status := wire.StatusOK
	if parsed, ok := wireprotocol.ParseErrorResponse(string(payload)); ok {
		status = wire.StatusError
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
			Message: joinErrorDetails(parsed.Details), Fields: fields,
		})
		if err != nil {
			return err
		}
	}
	kind := wire.KindResponse
	if c.request.Command == wire.CommandStream {
		kind = wire.KindStream
		if isStreamClosePayload(payload) {
			status = wire.StatusStreamEnd
		}
	}
	now := time.Now()
	deadline := c.requestDeadline
	if c.request.Command == wire.CommandStream || deadline.IsZero() || !deadline.After(now) {
		deadline = now.Add(c.writeTimeout)
	}
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return c.failWrite("set response write deadline", err)
	}
	err := c.connection.WriteFrame(wire.Frame{
		Kind: kind, Command: c.request.Command, Status: status, RequestID: c.request.RequestID, Payload: payload,
	})
	if err != nil {
		return c.failWrite("write response frame", err)
	}
	if err := c.Conn.SetWriteDeadline(time.Time{}); err != nil {
		return c.failWrite("clear response write deadline", err)
	}
	return nil
}

func (c *serverWireConn) failWrite(operation string, err error) error {
	reason := "error"
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		reason = "timeout"
	}
	metrics.ClientResponseWriteFailures.WithLabelValues(reason).Inc()
	_ = c.Conn.Close()
	return fmt.Errorf("%s: %w", operation, err)
}

func joinErrorDetails(details []string) string {
	return strings.Join(details, " ")
}

func isStreamClosePayload(payload []byte) bool {
	fields := strings.Fields(string(payload))
	if len(fields) == 0 || !strings.EqualFold(fields[0], "STREAM_CONTROL") {
		return false
	}
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if ok && strings.EqualFold(key, "type") && strings.EqualFold(value, "close") {
			return true
		}
	}
	return false
}

func negotiateServerConnection(conn net.Conn, writeTimeout time.Duration) (*wire.Connection, *serverWireConn, error) {
	connection, err := wire.ServerHandshake(conn, brokerCompressions)
	if err != nil {
		return nil, nil, err
	}
	return connection, newServerWireConn(conn, connection, writeTimeout), nil
}

func readWireRequestReserved(connection *wire.Connection, reserve wire.FrameReservation) (wire.Frame, func(), error) {
	frame, release, err := connection.ReadFrameReserved(reserve)
	if err != nil {
		return wire.Frame{}, func() {}, err
	}
	if frame.Kind != wire.KindRequest || frame.Status != wire.StatusNone || frame.RequestID == 0 {
		release()
		return wire.Frame{}, func() {}, fmt.Errorf("invalid Wire v2 request frame")
	}
	if wire.IsBatch(frame.Payload) {
		if frame.Command != wire.CommandPublish {
			release()
			return wire.Frame{}, func() {}, fmt.Errorf("wire v2 batch requires PUBLISH command")
		}
		return frame, release, nil
	}
	payload, err := wire.DecodeCommandPayload(frame.Payload)
	if err != nil {
		release()
		return wire.Frame{}, func() {}, fmt.Errorf("decode %s request: %w", frame.Command, err)
	}
	command, err := wire.RenderCommand(frame.Command, payload)
	if err != nil {
		release()
		return wire.Frame{}, func() {}, err
	}
	frame.Payload = []byte(command)
	return frame, release, nil
}

func (c *serverWireConn) SetDeadline(deadline time.Time) error {
	return c.Conn.SetDeadline(deadline)
}
