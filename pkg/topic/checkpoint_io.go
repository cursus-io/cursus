package topic

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// checkpointFileIO keeps fault injection local to a partition. The production
// path always performs write, file sync, atomic replacement and directory sync.
type checkpointFileIO interface {
	open(string) (checkpointFile, error)
	replace(string, string) error
	syncDir(string) error
}

type checkpointFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type osCheckpointIO struct{}

func (osCheckpointIO) open(path string) (checkpointFile, error) {
	// #nosec G304 -- path is derived from the broker-owned partition directory.
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

func (osCheckpointIO) replace(tmp, path string) error { return replaceCheckpointFile(tmp, path) }

func (osCheckpointIO) syncDir(path string) error {
	// Windows replacement uses MOVEFILE_WRITE_THROUGH instead.
	if runtime.GOOS == "windows" {
		return nil
	}
	// #nosec G304 -- path is derived from the broker-owned partition directory.
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func (p *Partition) writeCheckpoint(path string, data []byte) error {
	return p.writeCheckpointWithDirectorySync(path, data, nil)
}

func (p *Partition) writeCheckpointWithDirectorySync(path string, data []byte, syncDir func(string) error) error {
	ops := p.checkpointIO
	if ops == nil {
		ops = osCheckpointIO{}
	}
	tmp := path + ".tmp"
	f, err := ops.open(tmp)
	if err != nil {
		return fmt.Errorf("open checkpoint %s: %w", tmp, err)
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("write checkpoint %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync checkpoint %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint %s: %w", tmp, err)
	}
	if err := ops.replace(tmp, path); err != nil {
		return fmt.Errorf("replace checkpoint %s: %w", path, err)
	}
	if syncDir == nil {
		syncDir = ops.syncDir
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync checkpoint directory %s: %w", path, err)
	}
	return nil
}
