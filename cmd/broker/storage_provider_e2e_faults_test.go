//go:build e2e_faults

package main

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestFaultStorageProviderRequiresExplicitRuntimeGate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)

	t.Setenv("CURSUS_E2E_FAULTS", "")
	_, err := newStorageProvider(dm, cfg.LogDir)
	require.ErrorContains(t, err, "CURSUS_E2E_FAULTS=1")

	t.Setenv("CURSUS_E2E_FAULTS", "1")
	provider, err := newStorageProvider(dm, cfg.LogDir)
	require.NoError(t, err)
	require.NotSame(t, dm, provider)
}
