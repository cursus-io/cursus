package sdk

import (
	"errors"
	"net"
	"testing"
)

func TestAuthenticateConfiguredClientSendsExactCommand(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		request, err := ReadWithLength(server)
		if err != nil {
			done <- err
			return
		}
		if got := string(request[2:]); got != "AUTH principal=game-server token=secret-token" {
			done <- errors.New("unexpected auth command: " + got)
			return
		}
		done <- WriteWithLength(server, []byte("OK principal=game-server"))
	}()

	if err := authenticateConfiguredClient(client, "game-server", "secret-token"); err != nil {
		t.Fatalf("authenticate failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateConfiguredClientPreservesBrokerError(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = ReadWithLength(server)
		_ = WriteWithLength(server, []byte("ERROR: authentication_failed class=authorization retryable=false"))
	}()

	err := authenticateConfiguredClient(client, "game-server", "wrong-token")
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
