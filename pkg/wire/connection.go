package wire

import (
	"fmt"
	"net"
	"sync"
)

// Connection is a negotiated Wire v2 connection. It serializes frame reads
// and writes because a compressed frame must never be interleaved with another
// frame on the same stream.
type Connection struct {
	conn    net.Conn
	codec   *Codec
	readMu  sync.Mutex
	writeMu sync.Mutex
}

func ClientHandshake(conn net.Conn, compressions []Compression) (*Connection, error) {
	if conn == nil {
		return nil, fmt.Errorf("wire v2 client connection is nil")
	}
	request := NegotiationRequest{
		MinimumVersion: ProtocolVersion,
		MaximumVersion: ProtocolVersion,
		Compressions:   compressions,
	}
	payload, err := EncodeNegotiationRequest(request)
	if err != nil {
		return nil, err
	}
	plain, _ := NewCodec(CompressionNone)
	if err := plain.WriteFrame(conn, Frame{Kind: KindNegotiationRequest, Command: CommandNegotiate, Payload: payload}); err != nil {
		return nil, fmt.Errorf("send Wire v2 negotiation: %w", err)
	}
	frame, err := plain.ReadFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read Wire v2 negotiation: %w", err)
	}
	if frame.Kind != KindNegotiationResponse || frame.Command != CommandNegotiate {
		return nil, fmt.Errorf("unexpected Wire v2 negotiation frame kind=%d command=%s", frame.Kind, frame.Command)
	}
	if frame.Status != StatusOK {
		return nil, fmt.Errorf("wire v2 negotiation rejected")
	}
	response, err := DecodeNegotiationResponse(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode Wire v2 negotiation: %w", err)
	}
	selected := false
	for _, compression := range compressions {
		if compression == response.Compression {
			selected = true
			break
		}
	}
	if !selected {
		return nil, fmt.Errorf("broker selected unrequested compression %s", response.Compression)
	}
	codec, err := NewCodec(response.Compression)
	if err != nil {
		return nil, err
	}
	return &Connection{conn: conn, codec: codec}, nil
}

func ServerHandshake(conn net.Conn, supported []Compression) (*Connection, error) {
	if conn == nil {
		return nil, fmt.Errorf("wire v2 server connection is nil")
	}
	plain, _ := NewCodec(CompressionNone)
	frame, err := plain.ReadFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("read Wire v2 negotiation: %w", err)
	}
	if frame.Kind != KindNegotiationRequest || frame.Command != CommandNegotiate {
		return nil, fmt.Errorf("first frame must be a Wire v2 negotiation request")
	}
	request, err := DecodeNegotiationRequest(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode Wire v2 negotiation: %w", err)
	}
	compression, err := SelectCompression(request, supported)
	if err != nil {
		return nil, err
	}
	payload, err := EncodeNegotiationResponse(NegotiationResponse{Version: ProtocolVersion, Compression: compression})
	if err != nil {
		return nil, err
	}
	if err := plain.WriteFrame(conn, Frame{
		Kind: KindNegotiationResponse, Command: CommandNegotiate, Status: StatusOK, RequestID: frame.RequestID, Payload: payload,
	}); err != nil {
		return nil, fmt.Errorf("send Wire v2 negotiation: %w", err)
	}
	codec, err := NewCodec(compression)
	if err != nil {
		return nil, err
	}
	return &Connection{conn: conn, codec: codec}, nil
}

func (c *Connection) Compression() Compression {
	if c == nil || c.codec == nil {
		return CompressionNone
	}
	return c.codec.Compression()
}

func (c *Connection) ReadFrame() (Frame, error) {
	if c == nil || c.conn == nil || c.codec == nil {
		return Frame{}, fmt.Errorf("wire v2 connection is not initialized")
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	return c.codec.ReadFrame(c.conn)
}

func (c *Connection) WriteFrame(frame Frame) error {
	if c == nil || c.conn == nil || c.codec == nil {
		return fmt.Errorf("wire v2 connection is not initialized")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.codec.WriteFrame(c.conn, frame)
}
