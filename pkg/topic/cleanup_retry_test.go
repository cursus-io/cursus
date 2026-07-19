package topic

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

type cleanupRetryProvider struct {
	*disk.DiskManager
	fail bool
}

func (provider *cleanupRetryProvider) RemoveTopicStorage(path string) error {
	if provider.fail {
		return errors.New("injected cleanup failure")
	}
	return provider.DiskManager.RemoveTopicStorage(path)
}

func TestTopicStorageCleanupFailureCanBeRetriedAfterLogicalDelete(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	provider := &cleanupRetryProvider{DiskManager: dm}
	manager := NewTopicManager(cfg, provider, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))

	provider.fail = true

	deleted, err := manager.DeleteTopicDurable("orders")
	require.True(t, deleted)
	require.ErrorContains(t, err, "injected cleanup failure")
	require.Nil(t, manager.GetTopic("orders"))
	require.DirExists(t, filepath.Join(cfg.LogDir, "orders"))

	provider.fail = false
	require.NoError(t, manager.CleanupTopicStorage("orders"))
	require.NoDirExists(t, filepath.Join(cfg.LogDir, "orders"))
	require.NoError(t, manager.CleanupTopicStorage("orders"))

	require.NoError(t, manager.CreateTopic("payments", 1, false, false))
	provider.fail = true
	require.True(t, manager.DeleteTopic("payments"), "logical deletion must remain visible when only cleanup fails")
	provider.fail = false
	require.NoError(t, manager.CleanupTopicStorage("payments"))
}
