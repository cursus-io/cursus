package sdk

import (
	"errors"
	"net"
	"testing"
)

func TestAuthenticateConfiguredClientSendsExactCommand(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	done := make(chan error, 1)
	go func() {
		connection, request, command, err := acceptWireTestRequest(server)
		if err != nil {
			done <- err
			return
		}
		if command != "AUTH principal=game-server token=secret-token" {
			done <- errors.New("unexpected auth command: " + command)
			return
		}
		done <- writeWireTestResponse(connection, request, "OK principal=game-server")
	}()

	framed, err := openWireConnection(client, 1000, "none")
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateConfiguredClient(framed, "game-server", "secret-token"); err != nil {
		t.Fatalf("authenticate failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateConfiguredClientPreservesBrokerError(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	go func() {
		connection, request, _, err := acceptWireTestRequest(server)
		if err == nil {
			err = writeWireTestResponse(connection, request, "ERROR: authentication_failed class=authorization retryable=false")
		}
		if err != nil {
			_ = server.Close()
		}
	}()

	framed, err := openWireConnection(client, 1000, "none")
	if err != nil {
		t.Fatal(err)
	}
	err = authenticateConfiguredClient(framed, "game-server", "wrong-token")
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("expected BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != "authentication_failed" || brokerErr.Class != ErrorClassAuthorization {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
}

func TestAuthenticateConfiguredClientValidatesCredentialPair(t *testing.T) {
	if err := authenticateConfiguredClient(nil, "", ""); err != nil {
		t.Fatalf("empty credentials should disable authentication: %v", err)
	}
	for _, test := range []struct {
		principal string
		token     string
	}{
		{principal: "game-server"},
		{token: "secret-token"},
		{principal: "game server", token: "secret-token"},
		{principal: "game-server", token: "secret token"},
	} {
		if err := authenticateConfiguredClient(nil, test.principal, test.token); err == nil {
			t.Fatalf("invalid credentials accepted: principal=%q token=%q", test.principal, test.token)
		}
	}
}
