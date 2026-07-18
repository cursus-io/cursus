package sdk

import (
	"fmt"
	"net"
	"strings"
	"time"
)

const authenticationTimeout = 5 * time.Second

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
	if err := conn.SetDeadline(time.Now().Add(authenticationTimeout)); err != nil {
		return fmt.Errorf("set authentication deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	cmd := fmt.Sprintf("AUTH principal=%s token=%s", principal, token)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return fmt.Errorf("send authentication command: %w", err)
	}
	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read authentication response: %w", err)
	}
	value := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(value); ok {
		return brokerErr
	}
	if _, err := parseOKResponse(value); err != nil {
		return fmt.Errorf("unexpected authentication response: %s", value)
	}
	return nil
}
