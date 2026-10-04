package disk

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDiskHandlerRejectsSecondWriterAndPreservesAcknowledgedData(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	first, err := NewDiskHandler(cfg, "exclusive", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	offset, err := first.AppendMessageSync("exclusive", 0, &types.Message{Payload: "first"})
	require.NoError(t, err)
	require.Zero(t, offset)
	before, err := os.ReadFile(first.GetSegmentPath(0))
	require.NoError(t, err)
	second, err := NewDiskHandler(cfg, "exclusive", 0)
	if second != nil {
		_ = second.Close()
	}
	require.ErrorContains(t, err, "exclusive storage lock")
	after, err := os.ReadFile(first.GetSegmentPath(0))
	require.NoError(t, err)
	require.Equal(t, before, after, "rejected writer must not run recovery or mutate the log")
	offset, err = first.AppendMessageSync("exclusive", 0, &types.Message{Payload: "second"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), offset)
	require.NoError(t, first.Close())
	reopened, err := NewDiskHandler(cfg, "exclusive", 0)
	require.NoError(t, err)
	defer reopened.Close()
	messages, err := reopened.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, "first", messages[0].Payload)
	require.Equal(t, "second", messages[1].Payload)
}

func TestDiskHandlerConstructionFailureReleasesLock(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir())
	// A directory in place of the segment forces failure after locking.
	segment := filepath.Join(cfg.LogDir, "failed", "partition_0_segment_00000000000000000000.log")
	require.NoError(t, os.MkdirAll(segment, 0o750))
	_, err := NewDiskHandler(cfg, "failed", 0)
	require.Error(t, err)
	require.NoError(t, os.Remove(segment))
	handler, err := NewDiskHandler(cfg, "failed", 0)
	require.NoError(t, err)
	require.NoError(t, handler.Close())
}

func TestStorageLockProcessHelper(t *testing.T) {
	dir := os.Getenv("CURSUS_STORAGE_LOCK_HELPER_DIR")
	if dir == "" {
		return
	}
	lock, err := LockStorageDirectory(dir)
	if os.Getenv("CURSUS_STORAGE_LOCK_HELPER_MODE") == "probe" {
		if err == nil {
			_ = lock.Close()
			t.Fatal("second process acquired an owned storage directory")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	fmt.Println("LOCKED")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestStorageDirectoryLockIsProcessExclusive(t *testing.T) {
	dir := t.TempDir()
	lock, err := LockStorageDirectory(dir)
	require.NoError(t, err)
	defer lock.Close()
	executable, err := os.Executable()
	require.NoError(t, err)
	child := exec.Command(executable, "-test.run=^TestStorageLockProcessHelper$")
	child.Env = append(os.Environ(), "CURSUS_STORAGE_LOCK_HELPER_DIR="+dir, "CURSUS_STORAGE_LOCK_HELPER_MODE=probe")
	output, err := child.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.NoError(t, lock.Close())
	reopened, err := LockStorageDirectory(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestStorageDirectoryLockIsReleasedWhenOwnerIsKilled(t *testing.T) {
	dir := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	child := exec.Command(executable, "-test.run=^TestStorageLockProcessHelper$")
	child.Env = append(os.Environ(), "CURSUS_STORAGE_LOCK_HELPER_DIR="+dir, "CURSUS_STORAGE_LOCK_HELPER_MODE=hold")
	input, err := child.StdinPipe()
	require.NoError(t, err)
	defer input.Close()
	output, err := child.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(output).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		require.Contains(t, line, "LOCKED")
	case <-time.After(5 * time.Second):
		t.Fatal("child did not acquire the storage lock")
	}
	_, err = LockStorageDirectory(dir)
	require.Error(t, err)
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	lock, err := LockStorageDirectory(dir)
	require.NoError(t, err, "a stale lock file must not prevent crash recovery")
	require.NoError(t, lock.Close())
}
