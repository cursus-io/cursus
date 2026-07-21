package topic

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestTopicStorageCleanupFailureCanBeRetriedAfterLogicalDelete(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))

	originalRemove := removeTopicStorageFn
	fail := true
	removeTopicStorageFn = func(path string) error {
		if fail {
			return errors.New("injected cleanup failure")
		}
		return os.RemoveAll(path)
	}
	t.Cleanup(func() { removeTopicStorageFn = originalRemove })

	deleted, err := manager.DeleteTopicDurable("orders")
	require.True(t, deleted)
	require.ErrorContains(t, err, "injected cleanup failure")
	require.Nil(t, manager.GetTopic("orders"))
	require.DirExists(t, filepath.Join(cfg.LogDir, "orders"))

	fail = false
	require.NoError(t, manager.CleanupTopicStorage("orders"))
	require.NoDirExists(t, filepath.Join(cfg.LogDir, "orders"))
	require.NoError(t, manager.CleanupTopicStorage("orders"))

	require.NoError(t, manager.CreateTopic("payments", 1, false, false))
	fail = true
	require.True(t, manager.DeleteTopic("payments"), "logical deletion must remain visible when only cleanup fails")
	fail = false
	require.NoError(t, manager.CleanupTopicStorage("payments"))
}
