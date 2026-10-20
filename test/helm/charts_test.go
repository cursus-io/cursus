package helm_test

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func render(t *testing.T, chart string, overrides ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("Helm is required for chart rendering tests")
	}
	args := []string{"template", "cursus", filepath.Join("..", "..", "manifests", chart)}
	if chart == "helm" {
		args = append(args, "--set", "tls.enabled=true", "--set", "clientSecurity.sasl.secretName=client-auth")
	} else {
		args = append(args,
			"--set-string", "image.digest="+digest,
			"--set-string", "image.revision=0123456789abcdef0123456789abcdef01234567",
			"--set", "cluster.internalAuthSecret=internal-auth",
			"--set", "internalTLS.secretName=internal-tls",
			"--set", "clientSecurity.tls.secretName=client-tls",
			"--set", "clientSecurity.sasl.secretName=client-auth")
	}
	output, err := exec.Command(helm, append(args, overrides...)...).CombinedOutput()
	return strings.ReplaceAll(string(output), "\r\n", "\n"), err
}

func TestStandaloneDigestAndPerformanceOverrides(t *testing.T) {
	output, err := render(t, "helm", "--set-string", "image.digest="+digest,
		"--set", "image.tag=ignored", "--set", "performance.custom.lingerMS=0")
	if err != nil {
		t.Fatalf("render: %v\n%s", err, output)
	}
	for _, want := range []string{
		`image: "ghcr.io/cursus-io/cursus@` + digest + `"`,
		"linger_ms: 0", "disk_flush_batch_size: 500",
		"channel_buffer_size: 1024",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("missing %q in rendered chart", want)
		}
	}
	if strings.Contains(output, "cursus:ignored") {
		t.Error("tag overrode the pinned image digest")
	}
	for _, profile := range []struct{ name, linger string }{
		{"low-latency", "10"}, {"balanced", "100"}, {"high-throughput", "500"},
	} {
		t.Run(profile.name, func(t *testing.T) {
			output, err := render(t, "helm", "--set", "performanceProfile="+profile.name)
			if err != nil || !strings.Contains(output, "linger_ms: "+profile.linger+"\n") {
				t.Fatalf("profile render: %v\n%s", err, output)
			}
		})
	}
}

func TestChartSecurityDefaults(t *testing.T) {
	for _, chart := range []string{"helm", "helm-cluster"} {
		t.Run(chart, func(t *testing.T) {
			output, err := render(t, chart)
			if err != nil {
				t.Fatalf("render: %v\n%s", err, output)
			}
			for _, want := range []string{
				"automountServiceAccountToken: false", "runAsNonRoot: true",
				"readOnlyRootFilesystem: true", "allowPrivilegeEscalation: false",
				"type: RuntimeDefault", "- ALL",
			} {
				if !strings.Contains(output, want) {
					t.Errorf("missing security control %q", want)
				}
			}
		})
	}
}

func TestChartsRejectInvalidValues(t *testing.T) {
	for _, tc := range []struct {
		name, chart, flag, value, diagnostic string
	}{
		{"standalone-digest", "helm", "--set", "image.digest=invalid", "image.digest"},
		{"standalone-profile", "helm", "--set", "performanceProfile=typo", "performanceProfile"},
		{"standalone-auth-key", "helm", "--set", "clientSecurity.sasl.secretKey=", "clientSecurity.sasl.secretKey"},
		{"standalone-replicas", "helm", "--set", "replicaCount=2", "exactly one standalone"},
		{"standalone-insecure-string", "helm", "--set-string", "clientSecurity.allowInsecure=false", "must be a boolean"},
		{"cluster-mutable-string", "helm-cluster", "--set-string", "image.allowMutableTag=false", "image.allowMutableTag must be a boolean"},
		{"cluster-digest", "helm-cluster", "--set-string", "image.digest=invalid", "image.digest"},
		{"cluster-quorum", "helm-cluster", "--set", "cluster.minInSyncReplicas=1", "cluster.minInSyncReplicas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := render(t, tc.chart, tc.flag, tc.value)
			if err == nil || !strings.Contains(output, tc.diagnostic) {
				t.Fatalf("expected rejection containing %q, got %v\n%s", tc.diagnostic, err, output)
			}
		})
	}
}
