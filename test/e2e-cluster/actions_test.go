package e2e_cluster

import "testing"

func TestLeaderNodeFromDescribeUsesLeaderField(t *testing.T) {
	resp := `{
		"partitions": [{
			"leader": "broker-2:9001",
			"replicas": ["broker-1-9000", "broker-2-9000", "broker-3-9000"]
		}]
	}`

	got, err := leaderNodeFromDescribe(resp, 3)
	if err != nil {
		t.Fatalf("leaderNodeFromDescribe() error = %v", err)
	}
	if got != 2 {
		t.Fatalf("leaderNodeFromDescribe() = %d, want 2", got)
	}
}

func TestLeaderNodeFromDescribeRejectsUnknownLeader(t *testing.T) {
	resp := `{"partitions":[{"leader":"broker-4:9001"}]}`

	if _, err := leaderNodeFromDescribe(resp, 3); err == nil {
		t.Fatal("leaderNodeFromDescribe() error = nil, want out-of-range error")
	}
}

func TestValidateNodeHealthURLRestrictsLoopbackEndpoint(t *testing.T) {
	if err := validateNodeHealthURL(2, "http://localhost:9082/health"); err != nil {
		t.Fatalf("expected cluster health URL to be accepted: %v", err)
	}
	for _, candidate := range []string{
		"http://example.com:9082/health",
		"http://localhost:9081/health",
		"http://localhost:9082/other",
	} {
		if err := validateNodeHealthURL(2, candidate); err == nil {
			t.Fatalf("validateNodeHealthURL(%q) error = nil, want rejection", candidate)
		}
	}
}
