package sdk

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk/internal/transport"
)

func authenticateConfiguredClient(conn net.Conn, principal, token string) error {
	if principal == "" && token == "" {
		return nil
	}
	if principal == "" || token == "" {
		return fmt.Errorf("principal and auth token must be configured together")
	}
	if strings.ContainsAny(principal, " \t\r\n") {
		return fmt.Errorf("principal must not contain whitespace")
	}
	if strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("auth token must not contain whitespace")
	}
	if conn == nil {
		return fmt.Errorf("authentication connection is nil")
	}
	framed, ok := conn.(*transport.Conn)
	if !ok {
		return fmt.Errorf("authentication requires a Wire v2 connection")
	}
	if err := transport.Authenticate(framed, principal, token); err != nil {
		var brokerErr *wire.BrokerError
		if errors.As(err, &brokerErr) {
			return brokerErrorFromWire(brokerErr)
		}
		return err
	}
	return nil
}
