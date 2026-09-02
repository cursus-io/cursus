package sdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/sdk/internal/transport"
)

const defaultHandshakeTimeout = 5 * time.Second

// openWireConnection performs the sole protocol negotiation supported by the
// SDK: the Wire v2 handshake. The temporary deadline is cleared before the
// connection is returned so request-specific deadlines remain authoritative.
func openWireConnection(conn net.Conn, timeoutMS int, compression string) (net.Conn, error) {
	if conn == nil {
		return nil, fmt.Errorf("Wire v2 connection is nil")
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
		return conn, fmt.Errorf("Wire v2 handshake: %w", err)
	}
	return framed, nil
}

func parseOKResponse(response string) (map[string]string, error) {
	parts := strings.Fields(strings.TrimSpace(response))
	if len(parts) == 0 || parts[0] != "OK" {
		return nil, fmt.Errorf("unexpected response: %s", response)
	}
	fields := make(map[string]string, len(parts)-1)
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("malformed response field %q", part)
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate response field %q", key)
		}
		fields[key] = value
	}
	return fields, nil
}

func requiredIntField(fields map[string]string, key string) (int, error) {
	value, ok := fields[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", key, value)
	}
	return parsed, nil
}
