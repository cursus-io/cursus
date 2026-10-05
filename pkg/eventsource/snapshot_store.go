package eventsource

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/util"
)

var (
	snapshotV2Header = []byte("CRSSNP2\n")
	snapshotCRC      = crc32.MakeTable(crc32.Castagnoli)
)

const maxSnapshotPayloadBytes = 64 << 20

// snapshotPointer holds the location and identity of one visible snapshot.
// Legacy records remain readable, but every new record is written to the
// checksummed v2 companion file.
type snapshotPointer struct {
	fileOffset uint64
	version    uint64
	legacy     bool
}

type SnapshotData struct {
	Version uint64 `json:"version"`
	Payload string `json:"payload"`
}

type SnapshotRecord struct {
	Key     string
	Version uint64
	Payload string
}

// SnapshotInspection is the read-only backup inventory for application
// snapshots. LegacyFiles marks catalogs that still depend on the pre-checksum
// compatibility boundary.
type SnapshotInspection struct {
	Files       int `json:"files"`
	LegacyFiles int `json:"legacy_files"`
	Snapshots   int `json:"snapshots"`
}

type SnapshotStore struct {
	mu          sync.RWMutex
	file        *os.File
	legacyFile  *os.File
	index       map[string]*snapshotPointer
	syncFileFn  func() error
	writeOffset uint64
	revision    uint64
}

var syncSnapshotCreationDirectory = syncSnapshotDirectory

// InspectSnapshotCatalog validates every standalone snapshot file without
// creating, truncating, or repairing broker state.
func InspectSnapshotCatalog(logDir string) (SnapshotInspection, error) {
	inspection := SnapshotInspection{}
	patterns := []string{
		filepath.Join(logDir, "*", "partition_*_snapshots.dat"),
		filepath.Join(logDir, "*", "partition_*_snapshots_v2.dat"),
	}
	paths := make([]string, 0)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return inspection, fmt.Errorf("snapshot inventory pattern: %w", err)
		}
		paths = append(paths, matches...)
	}
	sort.Strings(paths)
	catalogs := make(map[string]*SnapshotStore)
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return inspection, fmt.Errorf("stat snapshot file %q: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return inspection, fmt.Errorf("snapshot file %q is not a regular file", path)
		}
		legacy := !bytes.HasSuffix([]byte(filepath.Base(path)), []byte("_v2.dat"))
		catalogKey := filepath.Join(filepath.Dir(path), strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), "_v2.dat"), ".dat"))
		store := catalogs[catalogKey]
		if store == nil {
			store = &SnapshotStore{index: make(map[string]*snapshotPointer), revision: 1}
			catalogs[catalogKey] = store
		}
		// #nosec G304 -- path comes from a bounded glob under the configured storage root.
		file, err := os.Open(path)
		if err != nil {
			return inspection, fmt.Errorf("open snapshot file %q: %w", path, err)
		}
		if legacy {
			store.legacyFile = file
			inspection.LegacyFiles++
			if err := store.scanFile(file, true, 0, false); err != nil {
				_ = file.Close()
				return inspection, fmt.Errorf("validate snapshot file %q: %w", path, err)
			}
		} else {
			store.file = file
			header := make([]byte, len(snapshotV2Header))
			if _, err := file.ReadAt(header, 0); err != nil || !bytes.Equal(header, snapshotV2Header) {
				_ = file.Close()
				return inspection, fmt.Errorf("validate snapshot file %q: invalid checksummed format header", path)
			}
			if err := store.scanFile(file, false, uint64(len(snapshotV2Header)), false); err != nil {
				_ = file.Close()
				return inspection, fmt.Errorf("validate snapshot file %q: %w", path, err)
			}
		}
		inspection.Files++
	}
	for _, store := range catalogs {
		inspection.Snapshots += len(store.index)
		if store.file != nil {
			_ = store.file.Close()
		}
		if store.legacyFile != nil {
			_ = store.legacyFile.Close()
		}
	}
	return inspection, nil
}

func NewSnapshotStore(dir string, partitionID int) (*SnapshotStore, error) {
	_, statErr := os.Stat(dir)
	dirCreated := os.IsNotExist(statErr)
	if statErr != nil && !dirCreated {
		return nil, fmt.Errorf("snapshot store: stat directory: %w", statErr)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("snapshot store: mkdir: %w", err)
	}

	legacyPath := filepath.Join(dir, fmt.Sprintf("partition_%d_snapshots.dat", partitionID))
	var legacyFile *os.File
	if _, err := os.Stat(legacyPath); err == nil {
		// #nosec G304 -- the name is fixed by the partition under the configured storage root.
		legacyFile, err = os.Open(legacyPath)
		if err != nil {
			return nil, fmt.Errorf("snapshot store: open legacy file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("snapshot store: stat legacy file: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("partition_%d_snapshots_v2.dat", partitionID))
	// #nosec G304 -- the name is fixed by the partition under the configured storage root.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	fileCreated := err == nil
	if errors.Is(err, os.ErrExist) {
		f, err = os.OpenFile(path, os.O_RDWR, 0o600)
	}
	if err != nil {
		if legacyFile != nil {
			_ = legacyFile.Close()
		}
		return nil, fmt.Errorf("snapshot store: open file: %w", err)
	}
	cleanup := func() {
		_ = f.Close()
		if legacyFile != nil {
			_ = legacyFile.Close()
		}
	}
	if fileCreated {
		if err := writeSnapshotFull(f, snapshotV2Header); err != nil {
			cleanup()
			_ = os.Remove(path)
			return nil, fmt.Errorf("snapshot store: write format header: %w", err)
		}
		if err := f.Sync(); err != nil {
			cleanup()
			_ = os.Remove(path)
			return nil, fmt.Errorf("snapshot store: sync new file: %w", err)
		}
	}
	if err := syncSnapshotCreationDirectory(dir); err != nil {
		cleanup()
		if fileCreated {
			_ = os.Remove(path)
		}
		return nil, fmt.Errorf("snapshot store: persist file entry: %w", err)
	}
	if err := syncSnapshotCreationDirectory(filepath.Dir(dir)); err != nil {
		cleanup()
		if fileCreated {
			_ = os.Remove(path)
			if dirCreated {
				_ = os.Remove(dir)
			}
		}
		return nil, fmt.Errorf("snapshot store: persist directory entry: %w", err)
	}

	s := &SnapshotStore{file: f, legacyFile: legacyFile, index: make(map[string]*snapshotPointer), revision: 1}
	if err := s.loadFromDisk(); err != nil {
		cleanup()
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

func (s *SnapshotStore) loadFromDisk() error {
	if s.legacyFile != nil {
		if err := s.scanFile(s.legacyFile, true, 0, false); err != nil {
			return fmt.Errorf("legacy snapshot file: %w", err)
		}
	}
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < int64(len(snapshotV2Header)) {
		return fmt.Errorf("checksummed snapshot header is truncated")
	}
	header := make([]byte, len(snapshotV2Header))
	if _, err := s.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("read snapshot header: %w", err)
	}
	if !bytes.Equal(header, snapshotV2Header) {
		return fmt.Errorf("unsupported snapshot format header %q", header)
	}
	if err := s.scanFile(s.file, false, uint64(len(snapshotV2Header)), true); err != nil {
		return err
	}
	info, err = s.file.Stat()
	if err != nil {
		return err
	}
	s.writeOffset = uint64(info.Size())
	return nil
}

func (s *SnapshotStore) scanFile(file *os.File, legacy bool, start uint64, repairTail bool) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 {
		return fmt.Errorf("negative snapshot file size")
	}
	end := uint64(info.Size())
	for offset := start; offset < end; {
		entryOffset := offset
		key, version, payload, next, err := readSnapshotRecord(file, offset, end, legacy)
		if err != nil {
			if repairTail && errors.Is(err, io.ErrUnexpectedEOF) {
				if err := file.Truncate(int64(entryOffset)); err != nil {
					return fmt.Errorf("truncate incomplete record at %d: %w", entryOffset, err)
				}
				if err := file.Sync(); err != nil {
					return fmt.Errorf("sync repaired snapshot file at %d: %w", entryOffset, err)
				}
				util.Warn("discarded incomplete snapshot record at offset %d", entryOffset)
				return nil
			}
			return fmt.Errorf("record at %d: %w", offset, err)
		}
		if ptr, ok := s.index[key]; ok {
			current, err := s.readAt(ptr)
			if err != nil {
				return err
			}
			switch {
			case version < current.Version:
				return fmt.Errorf("snapshot version regression for %q: current=%d incoming=%d", key, current.Version, version)
			case version == current.Version && payload != current.Payload:
				return fmt.Errorf("snapshot version collision for %q at version %d", key, version)
			case version == current.Version:
				offset = next
				continue
			}
		}
		s.index[key] = &snapshotPointer{fileOffset: entryOffset, version: version, legacy: legacy}
		s.revision++
		offset = next
	}
	return nil
}

func readSnapshotRecord(file *os.File, offset, fileSize uint64, legacy bool) (string, uint64, string, uint64, error) {
	const fixed = uint64(2 + 8 + 4)
	if fileSize-offset < fixed {
		return "", 0, "", offset, io.ErrUnexpectedEOF
	}
	var keyLenBuf [2]byte
	if _, err := file.ReadAt(keyLenBuf[:], int64(offset)); err != nil {
		return "", 0, "", offset, err
	}
	keyLen := uint64(binary.BigEndian.Uint16(keyLenBuf[:]))
	headerEnd := offset + 2 + keyLen + 8 + 4
	checksumBytes := uint64(0)
	if !legacy {
		checksumBytes = 4
	}
	if headerEnd > fileSize || fileSize-headerEnd < checksumBytes {
		return "", 0, "", offset, io.ErrUnexpectedEOF
	}
	header := make([]byte, 2+keyLen+8+4)
	if _, err := file.ReadAt(header, int64(offset)); err != nil {
		return "", 0, "", offset, err
	}
	versionPos := 2 + keyLen
	version := binary.BigEndian.Uint64(header[versionPos : versionPos+8])
	payloadLen := uint64(binary.BigEndian.Uint32(header[versionPos+8 : versionPos+12]))
	if payloadLen > maxSnapshotPayloadBytes {
		return "", 0, "", offset, fmt.Errorf("snapshot payload length %d exceeds limit", payloadLen)
	}
	recordEnd := headerEnd + payloadLen + checksumBytes
	if recordEnd > fileSize {
		return "", 0, "", offset, io.ErrUnexpectedEOF
	}
	payload := make([]byte, payloadLen)
	if _, err := file.ReadAt(payload, int64(headerEnd)); err != nil {
		return "", 0, "", offset, err
	}
	if !legacy {
		var checksum [4]byte
		if _, err := file.ReadAt(checksum[:], int64(headerEnd+payloadLen)); err != nil {
			return "", 0, "", offset, err
		}
		crc := crc32.New(snapshotCRC)
		_, _ = crc.Write(header)
		_, _ = crc.Write(payload)
		if crc.Sum32() != binary.BigEndian.Uint32(checksum[:]) {
			return "", 0, "", offset, fmt.Errorf("snapshot checksum mismatch")
		}
	}
	return string(header[2 : 2+keyLen]), version, string(payload), recordEnd, nil
}

func (s *SnapshotStore) Save(key string, version uint64, payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateSaveLocked(key, version, payload); err != nil {
		return err
	}
	if ptr, ok := s.index[key]; ok && ptr.version == version {
		return nil
	}
	keyBytes, payloadBytes := []byte(key), []byte(payload)
	keyLen, ok := util.SafeIntToUint16(len(keyBytes))
	if !ok {
		return fmt.Errorf("snapshot key length %d exceeds uint16 format limit", len(keyBytes))
	}
	payloadLen, ok := util.SafeIntToUint32(len(payloadBytes))
	if !ok || len(payloadBytes) > maxSnapshotPayloadBytes {
		return fmt.Errorf("snapshot payload length %d exceeds format limit", len(payloadBytes))
	}
	record := make([]byte, 2+len(keyBytes)+8+4+len(payloadBytes)+4)
	binary.BigEndian.PutUint16(record[0:2], keyLen)
	copy(record[2:], keyBytes)
	versionPos := 2 + len(keyBytes)
	binary.BigEndian.PutUint64(record[versionPos:versionPos+8], version)
	binary.BigEndian.PutUint32(record[versionPos+8:versionPos+12], payloadLen)
	copy(record[versionPos+12:], payloadBytes)
	checksumPos := len(record) - 4
	binary.BigEndian.PutUint32(record[checksumPos:], crc32.Checksum(record[:checksumPos], snapshotCRC))
	entryOffset := s.writeOffset
	if _, err := s.file.WriteAt(record, int64(entryOffset)); err != nil {
		return fmt.Errorf("snapshot store: write: %w", err)
	}
	if err := s.syncFile(); err != nil {
		rollbackErr := s.file.Truncate(int64(entryOffset))
		return errors.Join(fmt.Errorf("snapshot store: sync: %w", err), rollbackErr)
	}
	s.writeOffset += uint64(len(record))
	s.index[key] = &snapshotPointer{fileOffset: entryOffset, version: version}
	s.revision++
	return nil
}

// ValidateSave applies the same monotonicity and collision checks as Save
// without making the snapshot visible or touching disk.
func (s *SnapshotStore) ValidateSave(key string, version uint64, payload string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateSaveLocked(key, version, payload)
}

func (s *SnapshotStore) validateSaveLocked(key string, version uint64, payload string) error {
	if ptr, ok := s.index[key]; ok {
		current, err := s.readAt(ptr)
		if err != nil {
			return err
		}
		switch {
		case version < current.Version:
			return fmt.Errorf("snapshot version regression for %q: current=%d incoming=%d", key, current.Version, version)
		case version == current.Version && payload != current.Payload:
			return fmt.Errorf("snapshot version collision for %q at version %d", key, version)
		}
	}
	return nil
}

func (s *SnapshotStore) List() ([]SnapshotRecord, error) {
	return s.ListPage("", 0)
}

// ListPage returns a deterministic bounded page. afterKey is exclusive; a
// non-positive limit returns the complete catalog for compatibility.
func (s *SnapshotStore) ListPage(afterKey string, limit int) ([]SnapshotRecord, error) {
	records, _, _, err := s.ListPageAtRevision(afterKey, limit, 0)
	return records, err
}

// ListPageAtRevision keeps a multi-request catalog scan on one immutable
// revision. expectedRevision zero starts a new scan.
func (s *SnapshotStore) ListPageAtRevision(afterKey string, limit int, expectedRevision uint64) ([]SnapshotRecord, uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if expectedRevision != 0 && expectedRevision != s.revision {
		return nil, s.revision, false, fmt.Errorf("snapshot catalog changed: expected=%d current=%d", expectedRevision, s.revision)
	}
	keys := make([]string, 0, len(s.index))
	for key := range s.index {
		if key > afterKey {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	done := limit <= 0 || len(keys) <= limit
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	records := make([]SnapshotRecord, 0, len(keys))
	for _, key := range keys {
		ptr := s.index[key]
		snap, err := s.readAt(ptr)
		if err != nil {
			return nil, s.revision, false, err
		}
		if snap != nil {
			records = append(records, SnapshotRecord{Key: key, Version: snap.Version, Payload: snap.Payload})
		}
	}
	return records, s.revision, done, nil
}

func (s *SnapshotStore) Read(key string) (*SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ptr, ok := s.index[key]
	if !ok {
		return nil, nil
	}
	return s.readAt(ptr)
}

func (s *SnapshotStore) readAt(ptr *snapshotPointer) (*SnapshotData, error) {
	file := s.file
	if ptr.legacy {
		file = s.legacyFile
	}
	if file == nil {
		return nil, fmt.Errorf("snapshot backing file is unavailable")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	_, version, payload, _, err := readSnapshotRecord(file, ptr.fileOffset, uint64(info.Size()), ptr.legacy)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: read record: %w", err)
	}
	return &SnapshotData{Version: version, Payload: payload}, nil
}

func (s *SnapshotStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.file.Close()
	if s.legacyFile != nil {
		err = errors.Join(err, s.legacyFile.Close())
	}
	return err
}

func writeSnapshotFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
