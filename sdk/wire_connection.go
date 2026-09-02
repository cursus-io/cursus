package sdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cursus-io/cursus/sdk/internal/transport"
)

const defaultHandshakeTimeout = 5 * time.Second

func dialAuthenticatedWireConnection(
	ctx context.Context,
	addr string,
	dialTimeout time.Duration,
	handshakeTimeoutMS int,
	compression string,
	tlsConfig *tls.Config,
	principal string,
	authToken string,
) (net.Conn, error) {
	handshakeTimeout := defaultHandshakeTimeout
	if handshakeTimeoutMS > 0 {
		handshakeTimeout = time.Duration(handshakeTimeoutMS) * time.Millisecond
	}
	conn, err := transport.Dial(ctx, addr, transport.DialConfig{
		DialTimeout:      dialTimeout,
		HandshakeTimeout: handshakeTimeout,
		Compression:      compression,
		TLS:              tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	if err := authenticateConfiguredClient(conn, principal, authToken); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("authenticate with %s: %w", addr, err)
	}
	return conn, nil
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
