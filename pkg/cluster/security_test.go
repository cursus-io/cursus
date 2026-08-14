package cluster

import "testing"

func TestSecureClusterServerAuthenticatesWithoutExposingCommand(t *testing.T) {
	server := NewSecureClusterServer(nil, "secret-token", nil)
	if _, ok := server.authenticate("JOIN_CLUSTER {}"); ok {
		t.Fatal("unauthenticated cluster command was accepted")
	}
	if _, ok := server.authenticate("AUTH wrong JOIN_CLUSTER {}"); ok {
		t.Fatal("cluster command with wrong token was accepted")
	}
	command, ok := server.authenticate("AUTH secret-token JOIN_CLUSTER {}")
	if !ok || command != "JOIN_CLUSTER {}" {
		t.Fatalf("authenticated command = %q, accepted=%v", command, ok)
	}
	if got := clusterCommandName(command); got != "JOIN_CLUSTER" {
		t.Fatalf("command name = %q", got)
	}
}
