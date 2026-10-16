package eventsource

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
//
// NOTE: The on-disk index stores FNV-64a key hashes, not raw keys. In the rare event of
// a hash collision, after a restart, entries for colliding keys may appear under all keys
// that share the same hash. This is a known imprecision with negligible probability.
type StreamIndex struct {
	mu             sync.RWMutex
	invalid        error
	closed         bool
	resetRevision  uint64
	seeds          map[string]uint64
	seedAnchors    map[string]uint64
	offsetVersions map[string][]offsetVersion
	orderedRecords []indexedStreamRecord
	certificate    *snapshotIndexCertificate

	dir         string
	partitionID int

	indexFile   *os.File
	sidecarFile *os.File

	// In-memory cache for O(1) version lookup.
	states map[string]*streamState

	// In-memory entries for lookup queries.
	entries map[string][]StreamIndexEntry

	// Track which keys have been written to the sidecar.
	knownKeys map[string]bool
}

// NewStreamIndex opens or creates the index and sidecar files for the given partition,
// then loads existing data from disk into the in-memory cache.
func NewStreamIndex(dir string, partitionID int) (*StreamIndex, error) {
	indexPath := filepath.Join(dir, fmt.Sprintf("partition_%d_stream.idx", partitionID))
	sidecarPath := filepath.Join(dir, fmt.Sprintf("partition_%d_stream_keys.dat", partitionID))

	// #nosec G304 -- both file names are fixed by the partition and dir is the configured storage root.
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open index file: %w", err)
	}

	// #nosec G304 -- both file names are fixed by the partition and dir is the configured storage root.
	sidecarFile, err := os.OpenFile(sidecarPath, os.O_RDWR|os.O_CREATE, 0o600)
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
		knownKeys:   make(map[string]bool),
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

	hashToKeys := make(map[uint64][]string)
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
		// Handle hash collisions: store all keys that map to the same hash.
		if !si.knownKeys[key] {
			hashToKeys[hash] = append(hashToKeys[hash], key)
			si.knownKeys[key] = true
		}
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

		keys, ok := hashToKeys[entry.KeyHash]
		if !ok || len(keys) == 0 {
			// Skip entries with unknown keys (should not happen with valid data).
			continue
		}

		// If only one key maps to this hash, use it directly.
		// If multiple keys collide on the same hash, add the entry to all of them;
		// the in-memory entries will be resolved by the caller's key lookup.
		for _, key := range keys {
			si.entries[key] = append(si.entries[key], entry)
			si.rememberEntryLocked(key, entry)

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
	}

	// os.ReadFile above uses separate file descriptors, so neither open file's
	// cursor advances while rebuilding the in-memory cache. Move both cursors
	// to EOF before future Write calls; otherwise the first append after a
	// restart overwrites the first persisted record.
	if _, err := si.indexFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek index to end: %w", err)
	}
	if _, err := si.sidecarFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek sidecar to end: %w", err)
	}

	return nil
}

// CheckAndAppend atomically validates expected version and appends.
// Returns (true, nil) on success, (false, nil) on version conflict.
func (si *StreamIndex) CheckAndAppend(key string, expectedVersion, offset, position uint64) (bool, uint64, error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if err := si.checkLocked(); err != nil {
		return false, 0, err
	}

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

// CheckEnqueueAndAppend atomically validates expected version, enqueues via the
// provided callback, and appends the resulting offset to the index.
// This ensures no concurrent request can interleave between version check and append.
func (si *StreamIndex) CheckEnqueueAndAppend(key string, expectedVersion uint64, enqueueFn func() (uint64, error)) (bool, uint64, error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if err := si.checkLocked(); err != nil {
		return false, 0, err
	}

	current := uint64(0)
	if st, ok := si.states[key]; ok {
		current = st.currentVersion
	}

	if expectedVersion != current+1 {
		return false, current, nil
	}

	offset, err := enqueueFn()
	if err != nil {
		si.invalidateLocked(err)
		return false, current, err
	}

	if err := si.appendLocked(key, expectedVersion, offset, 0); err != nil {
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
func (si *StreamIndex) appendLocked(key string, aggregateVersion, offset, position uint64) (err error) {
	if err := si.checkLocked(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			si.invalidateLocked(err)
		}
	}()
	keyHash := util.GenerateID(key)

	// Write to sidecar on first occurrence of this key.
	if !si.knownKeys[key] {
		if err := si.writeSidecarEntry(keyHash, key); err != nil {
			return fmt.Errorf("write sidecar entry: %w", err)
		}
		si.knownKeys[key] = true
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

	if n, err := si.indexFile.Write(buf[:]); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	} else if n != len(buf) {
		return io.ErrShortWrite
	}

	// Update in-memory state.
	si.entries[key] = append(si.entries[key], entry)
	si.rememberEntryLocked(key, entry)

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
// maxSidecarKeyLen is the maximum allowed key length for sidecar entries.
// The sidecar format stores key length as a uint16, so the maximum is 65535.
const maxSidecarKeyLen = 65535

func (si *StreamIndex) writeSidecarEntry(hash uint64, key string) error {
	keyBytes := []byte(key)
	if len(keyBytes) > maxSidecarKeyLen {
		return fmt.Errorf("key length %d exceeds maximum %d bytes", len(keyBytes), maxSidecarKeyLen)
	}
	buf := make([]byte, 10+len(keyBytes))
	binary.BigEndian.PutUint64(buf[0:8], hash)
	keyLength, ok := util.SafeIntToUint16(len(keyBytes))
	if !ok {
		return fmt.Errorf("key length %d exceeds uint16 format limit", len(keyBytes))
	}
	binary.BigEndian.PutUint16(buf[8:10], keyLength)
	copy(buf[10:], keyBytes)

	if n, err := si.sidecarFile.Write(buf); err != nil {
		return err
	} else if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

// GetVersion is a compatibility accessor for tests. Zero means either a missing
// key or an unusable index. Production callers must use CheckedVersion so a
// failed index cannot authorize a new stream or snapshot as version zero.
func (si *StreamIndex) GetVersion(key string) uint64 {
	version, _ := si.CheckedVersion(key)
	return version
}

// CheckedVersion refuses to expose partial state after a failed write or rebuild.
func (si *StreamIndex) CheckedVersion(key string) (uint64, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if err := si.checkLocked(); err != nil {
		return 0, err
	}

	st, ok := si.states[key]
	if !ok {
		return 0, nil
	}
	return st.currentVersion, nil
}

var ErrStreamIndexInvalid = errors.New("stream index is invalid")

func (si *StreamIndex) checkLocked() error {
	if si.invalid != nil {
		return fmt.Errorf("%w: %v", ErrStreamIndexInvalid, si.invalid)
	}
	if si.closed {
		return fmt.Errorf("%w: closed", ErrStreamIndexInvalid)
	}
	return nil
}

func (si *StreamIndex) invalidateLocked(err error) {
	if si.invalid == nil {
		si.invalid = err
	}
}

// Invalidate is terminal: callers must retire this object and rebuild a new one.
func (si *StreamIndex) Invalidate(err error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if err == nil {
		err = ErrStreamIndexInvalid
	}
	si.invalidateLocked(err)
}

func (si *StreamIndex) ResetRevision() (uint64, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.resetRevision, si.checkLocked()
}

// SeedVersion establishes a retention checkpoint in the rebuilt in-memory
// index. It is derived from a durable application snapshot and is not written
// to the disposable stream index files.
func (si *StreamIndex) SeedVersion(key string, version uint64) error {
	return si.seedVersionAt(key, version, 0)
}

func (si *StreamIndex) seedVersionAt(key string, version, anchor uint64) error {
	if key == "" || version == 0 {
		return fmt.Errorf("invalid stream snapshot seed")
	}
	si.mu.Lock()
	defer si.mu.Unlock()
	if err := si.checkLocked(); err != nil {
		return err
	}
	if si.seeds[key] == version && si.seedAnchors[key] == anchor {
		return nil
	}
	if si.resetRevision == ^uint64(0) {
		si.invalidateLocked(fmt.Errorf("stream index seed revision exhausted"))
		return si.checkLocked()
	}
	if si.seeds == nil {
		si.seeds = make(map[string]uint64)
	}
	if si.seedAnchors == nil {
		si.seedAnchors = make(map[string]uint64)
	}
	si.seeds[key] = version
	si.seedAnchors[key] = anchor
	si.resetRevision++
	si.certificate = nil
	if current := si.states[key]; current == nil {
		si.states[key] = &streamState{currentVersion: version}
	} else if current.currentVersion < version {
		current.currentVersion = version
	}

	return nil
}

// Lookup returns all index entries for the given key with AggregateVersion >= fromVersion.
// Returns nil if the key is not found.
func (si *StreamIndex) Lookup(key string, fromVersion uint64) ([]StreamIndexEntry, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if err := si.checkLocked(); err != nil {
		return nil, err
	}

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

// LookupRange returns at most max entries in the inclusive version range. It
// uses the version-sorted in-memory index and copies only the requested page;
// callers must not use Lookup and trim the result after materializing an
// entire long aggregate history.
func (si *StreamIndex) LookupRange(key string, fromVersion, toVersion uint64, max int) ([]StreamIndexEntry, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if err := si.checkLocked(); err != nil {
		return nil, err
	}
	if fromVersion == 0 || toVersion < fromVersion || max <= 0 {
		return nil, nil
	}
	entries := si.entries[key]
	start := sort.Search(len(entries), func(i int) bool { return entries[i].AggregateVersion >= fromVersion })
	if start == len(entries) {
		return nil, nil
	}
	end := start
	for end < len(entries) && entries[end].AggregateVersion <= toVersion && end-start < max {
		end++
	}
	result := make([]StreamIndexEntry, end-start)
	copy(result, entries[start:end])
	return result, nil
}

// Reset clears the derived index so it can be rebuilt from the committed log.
func (si *StreamIndex) Reset() (err error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.resetLocked()
}

func (si *StreamIndex) resetLocked() (err error) {
	if err := si.checkLocked(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			si.invalidateLocked(err)
		}
	}()
	if si.resetRevision == ^uint64(0) {
		return fmt.Errorf("stream index reset revision exhausted")
	}
	si.resetRevision++

	if err := si.indexFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate index file: %w", err)
	}
	if _, err := si.indexFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek index file: %w", err)
	}
	if err := si.sidecarFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate sidecar file: %w", err)
	}
	if _, err := si.sidecarFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek sidecar file: %w", err)
	}
	if err := si.indexFile.Sync(); err != nil {
		return fmt.Errorf("sync index file: %w", err)
	}
	if err := si.sidecarFile.Sync(); err != nil {
		return fmt.Errorf("sync sidecar file: %w", err)
	}

	si.seeds = nil
	si.seedAnchors = nil
	si.offsetVersions = nil
	si.orderedRecords = nil
	si.certificate = nil
	si.states = make(map[string]*streamState)
	si.entries = make(map[string][]StreamIndexEntry)
	si.knownKeys = make(map[string]bool)
	return nil
}

// Close closes the underlying index and sidecar files.
func (si *StreamIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.closed {
		return nil
	}
	si.closed = true

	var firstErr error
	if err := si.indexFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := si.sidecarFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Sync durably flushes both populated index files before bootstrap publication.
func (si *StreamIndex) Sync() (err error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if err := si.checkLocked(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			si.invalidateLocked(err)
		}
	}()
	if err := si.indexFile.Sync(); err != nil {
		return err
	}
	return si.sidecarFile.Sync()
}
