package sdk

import (
	"net"
	"testing"
)

func TestConfiguredNegotiationRejectsEmptyRequiredFeatures(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if err := negotiateConfiguredProtocol(client, 0, nil, true, 10); err == nil {
		t.Fatal("empty required negotiation was accepted")
	}
}

func TestConfiguredNegotiationRemainsDisabledByDefault(t *testing.T) {
	if err := negotiateConfiguredProtocol(nil, 0, nil, false, 0); err != nil {
		t.Fatalf("default negotiation unexpectedly used connection: %v", err)
	}
}
