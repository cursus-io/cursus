package sdk

import (
	"net"
	"testing"
)

func TestConfiguredNegotiationRejectsEmptyRequiredFeatures(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	if _, err := negotiateConfiguredProtocol(client, 0, nil, true, 10, "none"); err == nil {
		t.Fatal("empty required negotiation was accepted")
	}
}

func TestConfiguredNegotiationRequiresConnection(t *testing.T) {
	if _, err := negotiateConfiguredProtocol(nil, 0, nil, false, 0, "none"); err == nil {
		t.Fatal("nil negotiation connection was accepted")
	}
}
