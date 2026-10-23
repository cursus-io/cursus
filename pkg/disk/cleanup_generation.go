package disk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// CleanupGeneration is a local durable cache fence, never replication authority.
// The storage lock excludes another handler. Prefix mutations overwrite and
// fsync this fixed-size file BEFORE touching data. Appends preserve the token.
// A lost new file only loses cache reuse; it cannot resurrect an old token.
func (d *DiskHandler) CleanupGeneration() (string, error) {
	return d.CleanupGenerationContext(context.Background())
}

func (d *DiskHandler) CleanupGenerationContext(ctx context.Context) (string, error) {
	unlock, err := lockProofContext(ctx, &d.mu)
	if err != nil {
		return "", err
	}
	defer unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return "", err
	}
	path := d.BaseName + ".cleanup-generation"
	// #nosec G304 -- BaseName is the broker-owned partition path.
	data, err := os.ReadFile(path)
	if err == nil && len(data) == 32 {
		return hex.EncodeToString(data), nil
	}
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return replaceCleanupGeneration(d.BaseName, true)
}

func replaceCleanupGeneration(base string, create bool) (string, error) {
	path := base + ".cleanup-generation"
	flags := os.O_WRONLY
	if create {
		flags |= os.O_CREATE
	}
	// #nosec G304 -- base is the broker-owned partition path.
	f, err := os.OpenFile(path, flags, 0o600)
	if !create && os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	data := make([]byte, 32)
	if _, err = rand.Read(data); err == nil {
		_, err = f.WriteAt(data, 0)
	}
	if err == nil {
		err = f.Truncate(32)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return "", fmt.Errorf("persist cleanup generation: %w", err)
	}
	return hex.EncodeToString(data), nil
}

// SyncCleanupCheckpointContext makes the captured committed prefix durable
// before a checkpoint is published. Ordinary replay batches need no fsync.
func (d *DiskHandler) SyncCleanupCheckpointContext(ctx context.Context) error {
	unlock, err := lockProofContext(ctx, &d.mu, &d.ioMu)
	if err != nil {
		return err
	}
	defer unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}
	if d.file != nil {
		if err := d.syncFile(d.file); err != nil {
			return d.markWriteUnavailable(err)
		}
	}
	return nil
}
