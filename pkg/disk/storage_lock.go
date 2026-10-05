package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// StorageLock keeps a kernel-managed exclusive lock until Close or process
// exit. The lock file remains in place: unlinking it could let another writer
// lock a different inode while an existing owner still holds the old one.
type StorageLock struct {
	file *os.File
	once sync.Once
	err  error
}

// LockStorageDirectory must be called before opening broker metadata or logs.
// It protects one broker's entire storage lifetime, including diagnostics mode.
func LockStorageDirectory(dir string) (*StorageLock, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("storage directory is empty")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}
	return lockStorageFile(filepath.Join(dir, ".cursus.lock"))
}

func lockStorageFile(path string) (*StorageLock, error) {
	// #nosec G304 -- the lock path is derived from the configured storage root.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open storage lock %s: %w", path, err)
	}
	if err := tryLockStorageFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("acquire exclusive storage lock %s (another writer may be active): %w", path, err)
	}
	return &StorageLock{file: file}, nil
}

func (lock *StorageLock) Close() error {
	if lock == nil {
		return nil
	}
	lock.once.Do(func() { lock.err = lock.file.Close() })
	return lock.err
}
