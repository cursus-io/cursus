package config

import (
	"strings"
	"testing"
)

func TestMarshalRedactedJSONRemovesCredentials(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InternalAuthToken = "cluster-secret"
	cfg.SASLUsers = []SASLUser{
		{Principal: "operator", Token: "operator-secret", Permissions: []string{"admin"}},
	}

	data, err := MarshalRedactedJSON(cfg)
	if err != nil {
		t.Fatalf("marshal redacted config: %v", err)
	}
	output := string(data)
	for _, secret := range []string{"cluster-secret", "operator-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("redacted config contains secret %q: %s", secret, output)
		}
	}
	if got := strings.Count(output, redactedConfigValue); got != 2 {
		t.Fatalf("redaction marker count = %d, want 2: %s", got, output)
	}
	if !strings.Contains(output, "operator") {
		t.Fatalf("non-secret principal missing from diagnostic output: %s", output)
	}
}
