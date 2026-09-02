package client

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestSecureClusterClientHasBoundedTimeoutAndAuthPrefix(t *testing.T) {
	client := NewSecureTCPClusterClient("secret-token", nil)
	if client.timeout <= 0 || client.timeout > 30*time.Second {
		t.Fatalf("cluster client timeout = %s", client.timeout)
	}
	fields := map[string]string{}
	requestFields := make(map[string]string, len(fields))
	requestFields["auth_token"] = client.authToken
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: requestFields})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodeCommandPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Fields["auth_token"] != "secret-token" {
		t.Fatalf("auth token field = %q", decoded.Fields["auth_token"])
	}
}
