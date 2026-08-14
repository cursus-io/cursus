package config

import "testing"

func TestValidateClusterTransportFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.InternalAuthToken = "cluster-secret"
	if err := cfg.ValidateClusterTransport(); err == nil {
		t.Fatal("plaintext distributed mode was accepted without explicit opt-in")
	}

	cfg.AllowInsecureClusterTransport = true
	if err := cfg.ValidateClusterTransport(); err != nil {
		t.Fatalf("isolated-test plaintext opt-in rejected: %v", err)
	}

	cfg.AllowInsecureClusterTransport = false
	cfg.InternalUseTLS = true
	if err := cfg.ValidateClusterTransport(); err != nil {
		t.Fatalf("mTLS cluster transport rejected: %v", err)
	}
}

func TestValidateClusterTransportRequiresToken(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.InternalUseTLS = true
	cfg.InternalAuthToken = ""
	if err := cfg.ValidateClusterTransport(); err == nil {
		t.Fatal("distributed mode without an auth token was accepted")
	}
}
