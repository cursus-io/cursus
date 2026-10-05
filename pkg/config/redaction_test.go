package config

import (
	"strings"
	"testing"
)

func TestMarshalRedactedJSONRemovesCredentials(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InternalAuthToken = "cluster-secret"
	cfg.InternalAuthTokenNext = "rotated-cluster-secret"
	cfg.ObservationGRPCAuthToken = "observation-secret"
	cfg.SASLUsers = []SASLUser{
		{Principal: "operator", Token: "operator-secret", Permissions: []string{"admin"}},
	}

	data, err := MarshalRedactedJSON(cfg)
	if err != nil {
		t.Fatalf("marshal redacted config: %v", err)
	}
	output := string(data)
	for _, secret := range []string{"cluster-secret", "rotated-cluster-secret", "observation-secret", "operator-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("redacted config contains secret %q: %s", secret, output)
		}
	}
	if got := strings.Count(output, redactedConfigValue); got != 4 {
		t.Fatalf("redaction marker count = %d, want 4: %s", got, output)
	}
	if !strings.Contains(output, "operator") {
		t.Fatalf("non-secret principal missing from diagnostic output: %s", output)
	}
}
