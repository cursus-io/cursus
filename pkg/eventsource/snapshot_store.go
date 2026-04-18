package eventsource

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

// SnapshotStore is a per-partition append-only snapshot store.
// Each entry on disk has the format:
//
//	[KeyLen:2][Key:K][Version:8][PayloadLen:4][Payload:P]
//
// The in-memory index keeps only the latest snapshot per key (last write wins).
type SnapshotStore struct {
	mu    sync.RWMutex
	file  *os.File
	index map[string]*snapshotPointer
	// writeOffset tracks the current end-of-file position for appends.
	writeOffset uint64
}

// NewSnapshotStore opens (or creates) the snapshot file for the given partition
// and rebuilds the in-memory index by scanning the file sequentially.
func NewSnapshotStore(dir string, partitionID int) (*SnapshotStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("snapshot store: mkdir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("partition_%d_snapshots.dat", partitionID))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: open file: %w", err)
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

// loadFromDisk scans the file sequentially and populates the in-memory index.
// For duplicate keys the last entry wins.
func (s *SnapshotStore) loadFromDisk() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var offset uint64
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
			return err
		}
		offset += uint64(keyLen)

		// Read Version (8 bytes).
		var version uint64
		if err := binary.Read(s.file, binary.BigEndian, &version); err != nil {
			return err
		}
		offset += 8

		// Read PayloadLen (4 bytes).
		var payloadLen uint32
		if err := binary.Read(s.file, binary.BigEndian, &payloadLen); err != nil {
			return err
		}
		offset += 4

		// Skip Payload bytes.
		if _, err := s.file.Seek(int64(payloadLen), io.SeekCurrent); err != nil {
			return err
		}
		offset += uint64(payloadLen)

		key := string(keyBuf)
		s.index[key] = &snapshotPointer{
			fileOffset: entryOffset,
			version:    version,
		}
	}

	s.writeOffset = offset
	return nil
}

// Save appends a new snapshot entry to the file and updates the in-memory index.
func (s *SnapshotStore) Save(key string, version uint64, payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyBytes := []byte(key)
	keyLen := uint16(len(keyBytes))
	payloadBytes := []byte(payload)
	payloadLen := uint32(len(payloadBytes))

	// Seek to the append position.
	if _, err := s.file.Seek(int64(s.writeOffset), io.SeekStart); err != nil {
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
	if err := s.file.Sync(); err != nil {
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
func (s *SnapshotStore) Read(key string) (*SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ptr, ok := s.index[key]
	if !ok {
		return nil, nil
	}

	// Seek to the entry's file offset.
	if _, err := s.file.Seek(int64(ptr.fileOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("snapshot store: seek: %w", err)
	}

	// Read KeyLen.
	var keyLen uint16
	if err := binary.Read(s.file, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("snapshot store: read key len: %w", err)
	}

	// Skip Key.
	if _, err := s.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, fmt.Errorf("snapshot store: skip key: %w", err)
	}

	// Read Version.
	var version uint64
	if err := binary.Read(s.file, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("snapshot store: read version: %w", err)
	}

	// Read PayloadLen.
	var payloadLen uint32
	if err := binary.Read(s.file, binary.BigEndian, &payloadLen); err != nil {
		return nil, fmt.Errorf("snapshot store: read payload len: %w", err)
	}

	// Read Payload.
	payloadBuf := make([]byte, payloadLen)
	if _, err := io.ReadFull(s.file, payloadBuf); err != nil {
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
