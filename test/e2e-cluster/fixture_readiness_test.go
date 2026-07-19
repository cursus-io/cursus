package e2e_cluster

import "testing"

func TestClusterReadinessFromListCluster(t *testing.T) {
	tests := []struct {
		name     string
		response string
		want     bool
	}{
		{
			name:     "active broker",
			response: `OK brokers=[{"id":"broker-1","status":"active"}]`,
			want:     true,
		},
		{
			name:     "error response",
			response: "ERROR: fsm_not_available",
		},
		{
			name:     "empty broker list",
			response: "OK brokers=[]",
		},
		{
			name:     "inactive brokers only",
			response: `OK brokers=[{"id":"broker-1","status":"inactive"}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ready, _, err := clusterReadinessFromListCluster(test.response)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ready != test.want {
				t.Fatalf("ready=%t want=%t response=%q", ready, test.want, test.response)
			}
		})
	}
}

func TestClusterReadinessFromListClusterRejectsMalformedBrokerJSON(t *testing.T) {
	ready, _, err := clusterReadinessFromListCluster("OK brokers=not-json")
	if err == nil {
		t.Fatal("expected malformed broker JSON error")
	}
	if ready {
		t.Fatal("malformed broker JSON reported ready")
	}
}
