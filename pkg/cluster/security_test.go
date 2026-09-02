package cluster

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestSecureClusterServerAuthenticatesWithoutExposingCommand(t *testing.T) {
	server := NewSecureClusterServer(nil, "secret-token", nil)
	if server.authenticate(wire.CommandPayload{Fields: map[string]string{}}) {
		t.Fatal("unauthenticated cluster command was accepted")
	}
	if server.authenticate(wire.CommandPayload{Fields: map[string]string{"auth_token": "wrong"}}) {
		t.Fatal("cluster command with wrong token was accepted")
	}
	if !server.authenticate(wire.CommandPayload{Fields: map[string]string{"auth_token": "secret-token"}}) {
		t.Fatal("cluster command with exact token was rejected")
	}
}
