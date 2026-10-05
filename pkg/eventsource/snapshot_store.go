package eventsource

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cursus-io/cursus/util"
)

// snapshotPointer holds the in-memory index entry for a snapshot.
type snapshotPointer struct {
	fileOffset uint64
	version    uint64
}

// SnapshotData represents a snapshot read back from the store.
type SnapshotData struct {
	Version uint64 `json:"version"`
	Payload string `json:"payload"`
}

// SnapshotRecord represents the latest snapshot for one aggregate key.
type SnapshotRecord struct {
	Key     string
	Version uint64
	Payload string
}

// SnapshotStore is a per-partition append-only snapshot store.
// Each entry on disk has the format:
//
//	[KeyLen:2][Key:K][Version:8][PayloadLen:4][Payload:P]
//
// The in-memory index keeps only the latest snapshot per key (last write wins).
type SnapshotStore struct {
	mu         sync.RWMutex
	file       *os.File
	index      map[string]*snapshotPointer
	syncFileFn func() error
	// writeOffset tracks the current end-of-file position for appends.
	writeOffset uint64
}

var syncSnapshotCreationDirectory = syncSnapshotDirectory

// NewSnapshotStore opens (or creates) the snapshot file for the given partition
// and rebuilds the in-memory index by scanning the file sequentially.
func NewSnapshotStore(dir string, partitionID int) (*SnapshotStore, error) {
	_, statErr := os.Stat(dir)
	dirCreated := os.IsNotExist(statErr)
	if statErr != nil && !dirCreated {
		return nil, fmt.Errorf("snapshot store: stat directory: %w", statErr)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("snapshot store: mkdir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("partition_%d_snapshots.dat", partitionID))
	// O_EXCL identifies the call that introduced the authoritative snapshot
	// filename. Existing files are opened without O_CREATE so later operations
	// cannot silently replace a missing store with an empty one.
	// #nosec G304 -- the file name is fixed by the partition and dir is the configured storage root.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	fileCreated := err == nil
	if os.IsExist(err) {
		// #nosec G304 -- path is the same broker-owned snapshot path validated above.
		f, err = os.OpenFile(path, os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("snapshot store: open file: %w", err)
	}
	if fileCreated {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("snapshot store: sync new file: %w", err)
		}
	}
	// Sync on every open to migrate files created by older releases. The parent
	// sync also persists the per-partition directory created by the handler.
	if err := syncSnapshotCreationDirectory(dir); err != nil {
		_ = f.Close()
		if fileCreated {
			_ = os.Remove(path)
		}
		return nil, fmt.Errorf("snapshot store: persist file entry: %w", err)
	}
	if err := syncSnapshotCreationDirectory(filepath.Dir(dir)); err != nil {
		_ = f.Close()
		if fileCreated {
			_ = os.Remove(path)
			if dirCreated {
				_ = os.Remove(dir)
			}
		}
		return nil, fmt.Errorf("snapshot store: persist directory entry: %w", err)
	}

	s := &SnapshotStore{
		file:  f,
		index: make(map[string]*snapshotPointer),
	}

	if err := s.loadFromDisk(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot store: load: %w", err)
	}

	return s, nil
}

func (s *SnapshotStore) syncFile() error {
	if s.syncFileFn != nil {
		return s.syncFileFn()
	}
	return s.file.Sync()
}

// loadFromDisk scans the file sequentially and populates the in-memory index.
// For duplicate keys the last entry wins.
func (s *SnapshotStore) loadFromDisk() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	fileInfo, err := s.file.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() < 0 {
		return fmt.Errorf("negative snapshot file size: %d", fileInfo.Size())
	}
	// #nosec G115 -- the file size is explicitly non-negative above.
	fileSize := uint64(fileInfo.Size())

	var offset uint64
	var lastGoodOffset uint64
	for {
		entryOffset := offset

		// Read KeyLen (2 bytes).
		var keyLen uint16
		if err := binary.Read(s.file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		offset += 2

		// Read Key.
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(s.file, keyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				// Truncate to last good entry.
				if truncErr := s.file.Truncate(int64(lastGoodOffset)); truncErr != nil {
					return fmt.Errorf("snapshot store: truncate after partial key: %w", truncErr)
				}
				break
			}
			return err
		}
		offset += uint64(keyLen)

		// Read Version (8 bytes).
		var version uint64
		if err := binary.Read(s.file, binary.BigEndian, &version); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if truncErr := s.file.Truncate(int64(lastGoodOffset)); truncErr != nil {
					return fmt.Errorf("snapshot store: truncate after partial version: %w", truncErr)
				}
				break
			}
			return err
		}
		offset += 8

		// Read PayloadLen (4 bytes).
		var payloadLen uint32
		if err := binary.Read(s.file, binary.BigEndian, &payloadLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if truncErr := s.file.Truncate(int64(lastGoodOffset)); truncErr != nil {
					return fmt.Errorf("snapshot store: truncate after partial payload len: %w", truncErr)
				}
				break
			}
			return err
		}
		offset += 4

		// Check that the declared payload fits in the file before seeking. Seeking
		// beyond EOF succeeds, so it cannot be used to detect a partial payload.
		if offset > fileSize || uint64(payloadLen) > fileSize-offset {
			if truncErr := s.file.Truncate(int64(lastGoodOffset)); truncErr != nil {
				return fmt.Errorf("snapshot store: truncate after partial payload: %w", truncErr)
			}
			break
		}

		// Skip the validated payload bytes.
		if _, err := s.file.Seek(int64(payloadLen), io.SeekCurrent); err != nil {
			return fmt.Errorf("snapshot store: seek payload: %w", err)
		}
		offset += uint64(payloadLen)

		key := string(keyBuf)
		s.index[key] = &snapshotPointer{
			fileOffset: entryOffset,
			version:    version,
		}
		lastGoodOffset = offset
	}

	s.writeOffset = lastGoodOffset
	return nil
}

// Save appends a new snapshot entry to the file and updates the in-memory index.
func (s *SnapshotStore) Save(key string, version uint64, payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyBytes := []byte(key)
	keyLen, ok := util.SafeIntToUint16(len(keyBytes))
	if !ok {
		return fmt.Errorf("snapshot key length %d exceeds uint16 format limit", len(keyBytes))
	}
	payloadBytes := []byte(payload)
	payloadLen, ok := util.SafeIntToUint32(len(payloadBytes))
	if !ok {
		return fmt.Errorf("snapshot payload length %d exceeds uint32 format limit", len(payloadBytes))
	}

	// Seek to the append position.
	writePosition, ok := util.SafeUint64ToInt64(s.writeOffset)
	if !ok {
		return fmt.Errorf("snapshot write offset %d exceeds file offset limit", s.writeOffset)
	}
	if _, err := s.file.Seek(writePosition, io.SeekStart); err != nil {
		return fmt.Errorf("snapshot store: seek: %w", err)
	}

	entryOffset := s.writeOffset

	// Write KeyLen.
	if err := binary.Write(s.file, binary.BigEndian, keyLen); err != nil {
		return fmt.Errorf("snapshot store: write key len: %w", err)
	}

	// Write Key.
	if _, err := s.file.Write(keyBytes); err != nil {
		return fmt.Errorf("snapshot store: write key: %w", err)
	}

	// Write Version.
	if err := binary.Write(s.file, binary.BigEndian, version); err != nil {
		return fmt.Errorf("snapshot store: write version: %w", err)
	}

	// Write PayloadLen.
	if err := binary.Write(s.file, binary.BigEndian, payloadLen); err != nil {
		return fmt.Errorf("snapshot store: write payload len: %w", err)
	}

	// Write Payload.
	if _, err := s.file.Write(payloadBytes); err != nil {
		return fmt.Errorf("snapshot store: write payload: %w", err)
	}

	// Sync to disk.
	if err := s.syncFile(); err != nil {
		return fmt.Errorf("snapshot store: sync: %w", err)
	}

	// Update write offset and index.
	s.writeOffset = entryOffset + 2 + uint64(keyLen) + 8 + 4 + uint64(payloadLen)
	s.index[key] = &snapshotPointer{
		fileOffset: entryOffset,
		version:    version,
	}

	return nil
}

// Read returns the latest snapshot for the given key, or nil if not found.
// List returns the latest snapshot for every aggregate key in this partition.
func (s *SnapshotStore) List() ([]SnapshotRecord, error) {
	s.mu.RLock()
	keys := make([]string, 0, len(s.index))
	for key := range s.index {
		keys = append(keys, key)
	}
	s.mu.RUnlock()

	sort.Strings(keys)
	records := make([]SnapshotRecord, 0, len(keys))
	for _, key := range keys {
		snap, err := s.Read(key)
		if err != nil {
			return nil, err
		}
		if snap == nil {
			continue
		}
		records = append(records, SnapshotRecord{Key: key, Version: snap.Version, Payload: snap.Payload})
	}
	return records, nil
}

func (s *SnapshotStore) Read(key string) (*SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ptr, ok := s.index[key]
	if !ok {
		return nil, nil
	}

	pos, ok := util.SafeUint64ToInt64(ptr.fileOffset)
	if !ok {
		return nil, fmt.Errorf("snapshot file offset %d exceeds file offset limit", ptr.fileOffset)
	}

	// Read KeyLen (2 bytes).
	var keyLenBuf [2]byte
	if _, err := s.file.ReadAt(keyLenBuf[:], pos); err != nil {
		return nil, fmt.Errorf("snapshot store: read key len: %w", err)
	}
	keyLen := binary.BigEndian.Uint16(keyLenBuf[:])
	pos += 2

	// Skip Key.
	pos += int64(keyLen)

	// Read Version (8 bytes).
	var versionBuf [8]byte
	if _, err := s.file.ReadAt(versionBuf[:], pos); err != nil {
		return nil, fmt.Errorf("snapshot store: read version: %w", err)
	}
	version := binary.BigEndian.Uint64(versionBuf[:])
	pos += 8

	// Read PayloadLen (4 bytes).
	var payloadLenBuf [4]byte
	if _, err := s.file.ReadAt(payloadLenBuf[:], pos); err != nil {
		return nil, fmt.Errorf("snapshot store: read payload len: %w", err)
	}
	payloadLen := binary.BigEndian.Uint32(payloadLenBuf[:])
	pos += 4

	// Read Payload.
	payloadBuf := make([]byte, payloadLen)
	if _, err := s.file.ReadAt(payloadBuf, pos); err != nil {
		return nil, fmt.Errorf("snapshot store: read payload: %w", err)
	}

	return &SnapshotData{
		Version: version,
		Payload: string(payloadBuf),
	}, nil
}

// Close closes the underlying file.
func (s *SnapshotStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
