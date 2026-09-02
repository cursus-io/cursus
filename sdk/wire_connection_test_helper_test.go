package sdk

import (
	"fmt"
	"net"
	"time"

	"github.com/cursus-io/cursus/sdk/internal/transport"
)

func openWireConnection(conn net.Conn, timeoutMS int, compression string) (net.Conn, error) {
	if conn == nil {
		return nil, fmt.Errorf("wire v2 connection is nil")
	}
	timeout := defaultHandshakeTimeout
	if timeoutMS > 0 {
		timeout = time.Duration(timeoutMS) * time.Millisecond
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return conn, fmt.Errorf("set Wire v2 handshake deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	framed, err := transport.NewClient(conn, compression)
	if err != nil {
		return conn, fmt.Errorf("wire v2 handshake: %w", err)
	}
	return framed, nil
}
