package client

import (
	"testing"
	"time"
)

func TestSecureClusterClientHasBoundedTimeoutAndAuthPrefix(t *testing.T) {
	client := NewSecureTCPClusterClient("secret-token", nil)
	if client.timeout <= 0 || client.timeout > 30*time.Second {
		t.Fatalf("cluster client timeout = %s", client.timeout)
	}
	if got := client.secureCommand("LIST_CLUSTER"); got != "AUTH secret-token LIST_CLUSTER" {
		t.Fatalf("secure command = %q", got)
	}
}
