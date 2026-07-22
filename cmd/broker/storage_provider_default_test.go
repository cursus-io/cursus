//go:build !e2e_faults

package main

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestDefaultStorageProviderReturnsRawDiskManager(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)

	provider, err := newStorageProvider(dm, cfg.LogDir)
	require.NoError(t, err)
	require.Same(t, dm, provider)
}
