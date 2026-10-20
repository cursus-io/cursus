package server

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/stretchr/testify/require"
)

func TestInternalClusterStatusRequiresAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		internal bool
		command  string
		want     string
	}{
		{"authenticated", true, "CLUSTER_STATUS internal_token=test-token", "distribution_required"},
		{"wrong token", true, "CLUSTER_STATUS internal_token=wrong", "internal_command_unauthorized"},
		{"missing token", true, "CLUSTER_STATUS", "internal_command_unauthorized"},
		{"extra argument", true, "CLUSTER_STATUS internal_token=test-token extra=x", "unknown_command"},
		{"public token argument", false, "CLUSTER_STATUS internal_token=test-token", "unknown_command"},
		{"public exact command", false, "CLUSTER_STATUS", "distribution_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.InternalAuthToken = "test-token"
			cfg.InternalUseTLS = false
			handler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
			t.Cleanup(func() { _ = handler.Close() })
			ctx := controller.NewClientContext("", 0)
			if tc.internal {
				ctx = controller.NewInternalClientContext("", 0)
			}
			client, server := newTestConnPair(t)
			done := make(chan error, 1)
			go func() {
				_, err := processMessage([]byte(tc.command), handler, ctx, server)
				done <- err
			}()
			require.Contains(t, string(readFramed(t, client)), tc.want)
			require.NoError(t, <-done)
		})
	}
}
