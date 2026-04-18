package eventsource

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cursus-io/cursus/util"
)

// StreamIndexEntry represents a fixed 32-byte entry on disk.
// Layout: [KeyHash:8][AggregateVersion:8][Offset:8][Position:8]
type StreamIndexEntry struct {
	KeyHash          uint64
	AggregateVersion uint64
	Offset           uint64
	Position         uint64
}

// StreamIndexEntrySize is the fixed byte size of each entry on disk.
const StreamIndexEntrySize = 32

// streamState tracks the current version and offset for an aggregate key.
type streamState struct {
	currentVersion uint64
	lastOffset     uint64
	entryCount     uint32
}

// StreamIndex is a per-partition index that maps aggregate keys to their event offsets.
// It maintains an on-disk index file and a sidecar file for key-to-hash recovery.
type StreamIndex struct {
	mu sync.RWMutex

	dir         string
	partitionID int

	indexFile   *os.File
	sidecarFile *os.File

	// In-memory cache for O(1) version lookup.
	states map[string]*streamState

	// In-memory entries for lookup queries.
	entries map[string][]StreamIndexEntry

	// Track which keys have been written to the sidecar.
	knownKeys map[uint64]string
}

// NewStreamIndex opens or creates the index and sidecar files for the given partition,
// then loads existing data from disk into the in-memory cache.
func NewStreamIndex(dir string, partitionID int) (*StreamIndex, error) {
	indexPath := filepath.Join(dir, fmt.Sprintf("partition_%d_stream.idx", partitionID))
	sidecarPath := filepath.Join(dir, fmt.Sprintf("partition_%d_stream_keys.dat", partitionID))

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open index file: %w", err)
	}

	sidecarFile, err := os.OpenFile(sidecarPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		_ = indexFile.Close()
		return nil, fmt.Errorf("open sidecar file: %w", err)
	}

	si := &StreamIndex{
		dir:         dir,
		partitionID: partitionID,
		indexFile:   indexFile,
		sidecarFile: sidecarFile,
		states:      make(map[string]*streamState),
		entries:     make(map[string][]StreamIndexEntry),
		knownKeys:   make(map[uint64]string),
	}

	if err := si.loadFromDisk(); err != nil {
		_ = indexFile.Close()
		_ = sidecarFile.Close()
		return nil, fmt.Errorf("load from disk: %w", err)
	}

	return si, nil
}

// loadFromDisk reads the sidecar file to rebuild the key-to-hash mapping,
// then reads the index file to rebuild the in-memory cache.
func (si *StreamIndex) loadFromDisk() error {
	// Read sidecar: each record is [hash:8][keyLen:2][keyBytes:N]
	sidecarData, err := os.ReadFile(si.sidecarFile.Name())
	if err != nil {
		return fmt.Errorf("read sidecar: %w", err)
	}

	hashToKey := make(map[uint64]string)
	pos := 0
	for pos < len(sidecarData) {
		if pos+10 > len(sidecarData) {
			break
		}
		hash := binary.BigEndian.Uint64(sidecarData[pos : pos+8])
		keyLen := binary.BigEndian.Uint16(sidecarData[pos+8 : pos+10])
		pos += 10
		if pos+int(keyLen) > len(sidecarData) {
			break
		}
		key := string(sidecarData[pos : pos+int(keyLen)])
		pos += int(keyLen)
		hashToKey[hash] = key
		si.knownKeys[hash] = key
	}

	// Read index file entries.
	indexData, err := os.ReadFile(si.indexFile.Name())
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}

	for i := 0; i+StreamIndexEntrySize <= len(indexData); i += StreamIndexEntrySize {
		entry := StreamIndexEntry{
			KeyHash:          binary.BigEndian.Uint64(indexData[i : i+8]),
			AggregateVersion: binary.BigEndian.Uint64(indexData[i+8 : i+16]),
			Offset:           binary.BigEndian.Uint64(indexData[i+16 : i+24]),
			Position:         binary.BigEndian.Uint64(indexData[i+24 : i+32]),
		}

		key, ok := hashToKey[entry.KeyHash]
		if !ok {
			// Skip entries with unknown keys (should not happen with valid data).
			continue
		}

		si.entries[key] = append(si.entries[key], entry)

		st, exists := si.states[key]
		if !exists {
			st = &streamState{}
			si.states[key] = st
		}
		if entry.AggregateVersion > st.currentVersion {
			st.currentVersion = entry.AggregateVersion
		}
		st.lastOffset = entry.Offset
		st.entryCount++
	}

	return nil
}

// CheckAndAppend atomically validates expected version and appends.
// Returns (true, nil) on success, (false, nil) on version conflict.
func (si *StreamIndex) CheckAndAppend(key string, expectedVersion, offset, position uint64) (bool, uint64, error) {
	si.mu.Lock()
	defer si.mu.Unlock()

	current := uint64(0)
	if st, ok := si.states[key]; ok {
		current = st.currentVersion
	}

	if expectedVersion != current+1 {
		return false, current, nil
	}

	if err := si.appendLocked(key, expectedVersion, offset, position); err != nil {
		return false, current, err
	}
	return true, expectedVersion, nil
}

// Append writes a new index entry for the given aggregate key.
// On the first append for a new key, the key-to-hash mapping is written to the sidecar.
func (si *StreamIndex) Append(key string, aggregateVersion, offset, position uint64) error {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.appendLocked(key, aggregateVersion, offset, position)
}

// appendLocked writes an entry while the caller already holds si.mu.
func (si *StreamIndex) appendLocked(key string, aggregateVersion, offset, position uint64) error {
	keyHash := util.GenerateID(key)

	// Write to sidecar on first occurrence of this key.
	if _, exists := si.knownKeys[keyHash]; !exists {
		if err := si.writeSidecarEntry(keyHash, key); err != nil {
			return fmt.Errorf("write sidecar entry: %w", err)
		}
		si.knownKeys[keyHash] = key
	}

	entry := StreamIndexEntry{
		KeyHash:          keyHash,
		AggregateVersion: aggregateVersion,
		Offset:           offset,
		Position:         position,
	}

	// Write entry to disk.
	var buf [StreamIndexEntrySize]byte
	binary.BigEndian.PutUint64(buf[0:8], entry.KeyHash)
	binary.BigEndian.PutUint64(buf[8:16], entry.AggregateVersion)
	binary.BigEndian.PutUint64(buf[16:24], entry.Offset)
	binary.BigEndian.PutUint64(buf[24:32], entry.Position)

	if _, err := si.indexFile.Write(buf[:]); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}

	// Update in-memory state.
	si.entries[key] = append(si.entries[key], entry)

	st, exists := si.states[key]
	if !exists {
		st = &streamState{}
		si.states[key] = st
	}
	st.currentVersion = aggregateVersion
	st.lastOffset = offset
	st.entryCount++

	return nil
}

// writeSidecarEntry writes a key-to-hash mapping: [hash:8][keyLen:2][keyBytes:N]
func (si *StreamIndex) writeSidecarEntry(hash uint64, key string) error {
	keyBytes := []byte(key)
	buf := make([]byte, 10+len(keyBytes))
	binary.BigEndian.PutUint64(buf[0:8], hash)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(keyBytes)))
	copy(buf[10:], keyBytes)

	if _, err := si.sidecarFile.Write(buf); err != nil {
		return err
	}
	return nil
}

// GetVersion returns the current aggregate version for the given key.
// Returns 0 if the key is not found.
func (si *StreamIndex) GetVersion(key string) uint64 {
	si.mu.RLock()
	defer si.mu.RUnlock()

	st, ok := si.states[key]
	if !ok {
		return 0
	}
	return st.currentVersion
}

// Lookup returns all index entries for the given key with AggregateVersion >= fromVersion.
// Returns nil if the key is not found.
func (si *StreamIndex) Lookup(key string, fromVersion uint64) ([]StreamIndexEntry, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	entries, ok := si.entries[key]
	if !ok {
		return nil, nil
	}

	var result []StreamIndexEntry
	for _, e := range entries {
		if e.AggregateVersion >= fromVersion {
			result = append(result, e)
		}
	}
	return result, nil
}

// Close closes the underlying index and sidecar files.
func (si *StreamIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	var firstErr error
	if err := si.indexFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := si.sidecarFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
