package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"golang.org/x/exp/mmap"
)

func (d *DiskHandler) openIndexFiles() error {
	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			return fmt.Errorf("failed to close existing index file: %w", err)
		}
		d.indexFile = nil
	}
	indexPath := d.GetIndexPath(d.CurrentSegment)

	info, statErr := os.Stat(indexPath)
	var isNew bool
	var existingSize uint64
	if os.IsNotExist(statErr) {
		isNew = true
	} else if statErr == nil {
		existingSize = uint64(info.Size())
	}

	isReadOnly := false
	f, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		if os.IsPermission(err) {
			util.Debug("Index file %s is read-only, opening in read-only mode", indexPath)
			f, err = os.Open(indexPath)
			if err != nil {
				return fmt.Errorf("failed to open read-only index: %w", err)
			}
			isReadOnly = true
		} else {
			return fmt.Errorf("failed to open index file: %w", err)
		}
	}

	if !isReadOnly {
		if err := f.Truncate(int64(d.IndexSize)); err != nil {
			if err := f.Close(); err != nil {
				util.Error("failed to close file: %v", err)
			}
			return fmt.Errorf("failed to truncate index file: %w", err)
		}
	}

	d.indexMu.Lock()
	defer d.indexMu.Unlock()

	d.indexFile = f
	if isNew {
		d.indexBytesWritten = 0
	} else {
		d.indexBytesWritten = d.seekLastValidIndexEntry(f, existingSize)
	}

	if !isReadOnly {
		d.indexWriter = bufio.NewWriter(f)
	}

	return d.refreshIndexMapper(indexPath)
}

func (d *DiskHandler) seekLastValidIndexEntry(f *os.File, fileSize uint64) uint64 {
	const entrySize = uint64(types.IndexEntrySize)
	if fileSize < entrySize {
		return 0
	}

	buf := make([]byte, entrySize)
	for pos := fileSize - entrySize; ; pos -= entrySize {
		_, err := f.ReadAt(buf, int64(pos))
		if err != nil {
			break
		}

		if binary.BigEndian.Uint64(buf[0:8]) != 0 || binary.BigEndian.Uint64(buf[8:16]) != 0 {
			return pos + entrySize
		}
		if pos == 0 {
			break
		}
		if pos < entrySize {
			break
		}
	}
	return 0
}

func (d *DiskHandler) refreshIndexMapper(path string) error {
	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			util.Error("failed to close indexMapper: %v", err)
		}
	}
	mapper, err := mmap.Open(path)
	if err != nil {
		return err
	}
	d.indexMapper = mapper
	return nil
}

func (d *DiskHandler) closeIndexFiles() error {
	var errs []error

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			return fmt.Errorf("flush index writer failed: %w", err)
		}
	}
	if d.indexFile != nil {
		if err := d.indexFile.Sync(); err != nil {
			return fmt.Errorf("sync index file failed: %w", err)
		}
	}

	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexMapper = nil
	}

	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexFile = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("closeIndexFiles errors: %v", errs)
	}
	return nil
}

func getLastOffsetFromIndex(indexPath string, baseOffset uint64) (lastOffset uint64, count int, err error) {
	const entrySize = types.IndexEntrySize
	info, err := os.Stat(indexPath)

	if err != nil || info.Size() < entrySize {
		return 0, 0, fmt.Errorf("index too small or not found")
	}

	f, err := os.Open(indexPath)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			util.Error("failed to close file: %v", err)
		}
	}()

	fileSize := uint64(info.Size())
	buf := make([]byte, entrySize)
	found := false

	for pos := fileSize - entrySize; ; pos -= entrySize {
		_, err := f.ReadAt(buf, int64(pos))
		if err != nil {
			break
		}

		if binary.BigEndian.Uint64(buf[0:8]) != 0 || binary.BigEndian.Uint64(buf[8:16]) != 0 {
			lastOffset = binary.BigEndian.Uint64(buf[0:8])
			found = true
			break
		}
		if pos < entrySize {
			break
		}
	}

	if !found {
		return 0, 0, fmt.Errorf("index empty (contains only zeros)")
	}
	if lastOffset < baseOffset {
		return 0, 0, fmt.Errorf("index empty or corrupt")
	}

	count = int(lastOffset - baseOffset + 1)
	return lastOffset, count, nil
}

// findSegmentForOffset finds which segment contains the given offset
func (dh *DiskHandler) findSegmentForOffset(offset uint64) (string, uint64, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	if offset > dh.AbsoluteOffset {
		return "", 0, fmt.Errorf("offset %d is out of bounds, latest is %d", offset, dh.AbsoluteOffset)
	}

	if offset >= dh.CurrentSegment {
		return dh.GetSegmentPath(dh.CurrentSegment), dh.CurrentSegment, nil
	}

	if len(dh.segments) == 0 {
		return "", 0, fmt.Errorf("no segments available")
	}

	idx := sort.Search(len(dh.segments), func(i int) bool {
		return dh.segments[i] > offset
	})

	var baseOffset uint64
	if idx > 0 {
		baseOffset = dh.segments[idx-1]
	} else {
		baseOffset = dh.segments[0]
	}
	return dh.GetSegmentPath(baseOffset), baseOffset, nil
}

func (d *DiskHandler) findOffsetPosition(offset uint64) (uint64, error) {
	d.indexMu.RLock()
	defer d.indexMu.RUnlock()

	if d.indexMapper == nil {
		return 0, fmt.Errorf("index not available")
	}

	const entrySize = types.IndexEntrySize
	entryCount := d.indexBytesWritten / entrySize

	if entryCount == 0 {
		return 0, nil
	}

	low, high := 0, int(entryCount)-1
	var lastFoundPos uint64 = 0
	buf := make([]byte, entrySize)

	for low <= high {
		mid := (low + high) / 2
		_, err := d.indexMapper.ReadAt(buf, int64(mid*entrySize))
		if err != nil {
			break
		}

		eOffset := binary.BigEndian.Uint64(buf[0:8])
		ePosition := binary.BigEndian.Uint64(buf[8:16])

		if eOffset == offset {
			return ePosition, nil
		} else if eOffset < offset {
			lastFoundPos = ePosition
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return lastFoundPos, nil
}
