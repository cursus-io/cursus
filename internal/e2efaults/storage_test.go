//go:build e2e_faults

package e2efaults

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestStorageProviderInjectsTopicScopedHandlerOpenFailure(t *testing.T) {
	provider, logRoot, sentinelRoot := newTestStorageProvider(t)

	setSentinel(t, sentinelRoot, HandlerOpenOperation, "orders")
	_, err := provider.GetHandlerWithPolicy("orders", 0, config.CleanupPolicyDelete, 1, 1024)
	require.ErrorContains(t, err, "operation=handler-open topic=orders")

	payments, err := provider.GetHandler("payments", 0)
	require.NoError(t, err)
	require.NotNil(t, payments)

	require.NoError(t, os.Remove(filepath.Join(sentinelRoot, HandlerOpenOperation, "orders")))
	orders, err := provider.GetHandler("orders", 0)
	require.NoError(t, err)
	require.NotNil(t, orders)
	require.DirExists(t, logRoot)
}

func TestStorageProviderInjectsRetriablePhysicalCleanupFailure(t *testing.T) {
	provider, logRoot, sentinelRoot := newTestStorageProvider(t)
	target := filepath.Join(logRoot, "orders")
	require.NoError(t, os.MkdirAll(target, 0o750))
	setSentinel(t, sentinelRoot, CleanupOperation, "orders")

	err := provider.RemoveTopicStorage(target)
	require.ErrorContains(t, err, "operation=cleanup topic=orders")
	require.DirExists(t, target)

	require.NoError(t, os.Remove(filepath.Join(sentinelRoot, CleanupOperation, "orders")))
	require.NoError(t, provider.RemoveTopicStorage(target))
	require.NoDirExists(t, target)
	require.NoError(t, provider.RemoveTopicStorage(target))
}

func TestStorageProviderRejectsCleanupOutsideLogRoot(t *testing.T) {
	provider, _, _ := newTestStorageProvider(t)
	err := provider.RemoveTopicStorage(filepath.Join(t.TempDir(), "orders"))
	require.ErrorContains(t, err, "outside direct log-root child")
}

func TestNewStorageProviderRequiresRuntimeGate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)

	t.Setenv(RuntimeGateEnvironment, "")
	_, err := NewStorageProvider(dm, cfg.LogDir)
	require.ErrorContains(t, err, RuntimeGateEnvironment+"=1")
}

func newTestStorageProvider(t *testing.T) (*StorageProvider, string, string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	sentinelRoot := t.TempDir()
	provider, err := newStorageProvider(dm, cfg.LogDir, sentinelRoot)
	require.NoError(t, err)
	return provider, cfg.LogDir, sentinelRoot
}

func setSentinel(t *testing.T, root, operation, topicName string) {
	t.Helper()
	dir := filepath.Join(root, operation)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	file, err := os.OpenFile(filepath.Join(dir, topicName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
