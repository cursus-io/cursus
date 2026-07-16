package sdk

import "testing"

func TestAutoOffsetResetArgumentMatchesBrokerContract(t *testing.T) {
	tests := []struct {
		policy AutoOffsetResetPolicy
		want   string
	}{
		{policy: "", want: " autoOffsetReset=earliest"},
		{policy: AutoOffsetResetEarliest, want: " autoOffsetReset=earliest"},
		{policy: AutoOffsetResetLatest, want: " autoOffsetReset=latest"},
		{policy: AutoOffsetResetError, want: ""},
		{policy: "unsupported", want: " autoOffsetReset=earliest"},
	}
	for _, test := range tests {
		if got := autoOffsetResetArgument(test.policy); got != test.want {
			t.Fatalf("policy %q: want %q, got %q", test.policy, test.want, got)
		}
	}
}

func TestReadIsolationArgument(t *testing.T) {
	tests := []struct {
		isolation ReadIsolation
		want      string
	}{
		{isolation: "", want: " isolation=read_committed"},
		{isolation: ReadCommitted, want: " isolation=read_committed"},
		{isolation: ReadUncommitted, want: " isolation=read_uncommitted"},
		{isolation: "unsupported", want: " isolation=read_committed"},
	}
	for _, test := range tests {
		if got := readIsolationArgument(test.isolation); got != test.want {
			t.Fatalf("isolation %q: want %q, got %q", test.isolation, test.want, got)
		}
	}
}

func TestHasOKStatusRequiresExactToken(t *testing.T) {
	if !hasOKStatus("OK state=ready") {
		t.Fatal("valid OK response was rejected")
	}
	for _, response := range []string{"", "OKAY state=ready", "prefix OK", "ERROR: failed"} {
		if hasOKStatus(response) {
			t.Fatalf("invalid response accepted: %q", response)
		}
	}
}
