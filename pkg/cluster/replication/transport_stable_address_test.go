package replication

import "testing"

func TestStableRaftAddressPreservesConfiguredDNS(t *testing.T) {
	address := stableRaftAddress("broker-1.broker-headless.brokers.svc.cluster.local:9001")
	if got, want := address.Network(), "tcp"; got != want {
		t.Fatalf("Network() = %q, want %q", got, want)
	}
	if got, want := address.String(), "broker-1.broker-headless.brokers.svc.cluster.local:9001"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
