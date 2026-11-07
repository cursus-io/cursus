//go:build e2e_faults

package faultpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPauseRequiresGateAndExplicitRelease checks runtime gating, one-shot activation, and explicit release.
func TestPauseRequiresGateAndExplicitRelease(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CURSUS_E2E_FAULT_ROOT", root)
	t.Setenv("CURSUS_E2E_FAULTS", "0")
	path := filepath.Join(root, "commit", "orders-0-9")
	require.NoError(t, os.Mkdir(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, Pause("commit", "orders-0-9"))
	require.FileExists(t, path)
	t.Setenv("CURSUS_E2E_FAULTS", "1")
	done := make(chan error, 1)
	go func() { done <- Pause("commit", "orders-0-9") }()
	require.Eventually(t, func() bool { _, err := os.Stat(path + ".entered"); return err == nil }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("paused operation completed without release: %v", err)
	default:
	}
	require.NoError(t, os.WriteFile(path+".release", nil, 0o600))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("pause did not release")
	}
	require.NoError(t, Pause("commit", "orders-0-9"), "consumed point cannot fire after restart")
	require.Error(t, Pause("commit", "../escape"))
}
