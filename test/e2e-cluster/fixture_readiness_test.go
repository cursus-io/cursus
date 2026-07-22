package e2e_cluster

import "testing"

func TestListClusterReady(t *testing.T) {
	tests := []struct {
		name      string
		response  string
		wantReady bool
		wantError bool
	}{
		{
			name:      "success",
			response:  `OK brokers=[{"id":"broker-1","status":"active"}]`,
			wantReady: true,
		},
		{
			name:      "error response",
			response:  `ERROR: no_leader`,
			wantError: true,
		},
		{
			name:     "empty membership",
			response: `OK brokers=[]`,
		},
		{
			name:     "inactive membership",
			response: `OK brokers=[{"id":"broker-1","status":"inactive"}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ready, detail, err := listClusterReady(test.response)
			if ready != test.wantReady {
				t.Fatalf("listClusterReady(%q) ready = %t, want %t; detail=%s err=%v", test.response, ready, test.wantReady, detail, err)
			}
			if (err != nil) != test.wantError {
				t.Fatalf("listClusterReady(%q) error = %v, wantError=%t", test.response, err, test.wantError)
			}
			if detail == "" {
				t.Fatal("listClusterReady returned empty diagnostic detail")
			}
		})
	}
}
