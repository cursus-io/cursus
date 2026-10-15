package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cursus-io/cursus/pkg/types"
)

// Incoming verification is shared by durable installation and detached preparation.
func verifyBootstrapIncoming(ctx context.Context, t *bootstrapTransfer) error {
	for _, f := range t.metadata.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if t.received[f.ID] != f.Size {
			return fmt.Errorf("bootstrap file is incomplete")
		}
		path := filepath.Join(t.dir, f.ID)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("bootstrap file is not regular")
		}
		actual, err := describeBootstrapFileContext(ctx, path, f.ID, f.Role)
		if err != nil {
			return err
		}
		if actual != f {
			return fmt.Errorf("bootstrap file size or checksum mismatch")
		}
		// #nosec G304 -- validated fixed descriptor IDs select regular, checksum-verified files in the private staging directory.
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return ctx.Err()
}

func buildBootstrapCanonicalIndex(ctx context.Context, t *bootstrapTransfer, interval, emptyIndexBytes uint64) error {
	m := t.metadata
	indexPath := filepath.Join(t.dir, "range-index")
	// #nosec G304 -- the filename is fixed and t.dir is the privately generated staging directory.
	index, err := os.OpenFile(indexPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = index.Close() }()
	var position, lastIndex, next uint64
	next = m.StartOffset
	if interval == 0 {
		interval = 4096
	}
	err = scanCompactionSegmentContext(ctx, filepath.Join(t.dir, "records"), m.StartOffset, false, m.Compacted, func(frame compactionFrame) error {
		msg := frame.message
		if msg.Offset < m.StartOffset || msg.Offset >= m.EndOffset || msg.Topic != m.Fence.Topic || int(msg.Partition) != m.Fence.Partition {
			return fmt.Errorf("bootstrap record outside logical identity or bounds")
		}
		if !m.Compacted && msg.Offset != next {
			return fmt.Errorf("unmarked bootstrap offset gap")
		}
		next = msg.Offset + 1
		if t.detached != nil && position == 0 {
			t.detached.firstOffset = msg.Offset
		}
		if position-lastIndex >= interval {
			if err := binary.Write(index, binary.BigEndian, types.IndexEntry{Offset: msg.Offset, Position: position}); err != nil {
				return err
			}
			lastIndex = position
		}
		position += uint64(frame.size)
		return nil
	})
	if err != nil {
		return err
	}
	if !m.Compacted && next != m.EndOffset {
		return fmt.Errorf("unmarked bootstrap trailing offset gap")
	}
	if m.StartOffset == m.EndOffset && m.Files[0].Size != 0 {
		return fmt.Errorf("nonempty bootstrap empty range")
	}
	if m.StartOffset == m.EndOffset {
		// #nosec G115 -- callers pass zero or IndexSize admitted by StageBootstrap below the structural byte bound.
		if err = index.Truncate(int64(emptyIndexBytes)); err != nil {
			return err
		}
	}
	if err = index.Sync(); err != nil {
		return err
	}
	if t.detached != nil {
		t.detached.lastIndex = lastIndex
	}
	if err = index.Close(); err != nil {
		return err
	}
	return ctx.Err()
}
