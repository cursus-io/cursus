package transaction

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	maxJournalRecordBytes    = 32 << 20
	journalFormatVersion     = 2
	journalRecordOverhead    = 8
	journalCompactionBytes   = 16 << 20
	journalCompactionRecords = 256
)

type journalRecord struct {
	Version           int       `json:"version"`
	Transaction       *Snapshot `json:"transaction,omitempty"`
	NextProducerEpoch *uint64   `json:"next_producer_epoch,omitempty"`
}

// JournalInspection is a read-only integrity summary for a standalone journal.
type JournalInspection struct {
	Present                   bool   `json:"present"`
	RecordCount               int    `json:"record_count"`
	LatestTransactions        int    `json:"latest_transactions"`
	NextProducerEpoch         uint64 `json:"next_producer_epoch"`
	HasProducerEpochWatermark bool   `json:"has_producer_epoch_watermark"`
}

type journalManifest struct {
	Version                   int    `json:"version"`
	State                     string `json:"state"`
	JournalSize               int64  `json:"journal_size"`
	RecordCount               int    `json:"record_count"`
	LatestTransactions        int    `json:"latest_transactions"`
	NextProducerEpoch         uint64 `json:"next_producer_epoch"`
	HasProducerEpochWatermark bool   `json:"has_producer_epoch_watermark"`
	Checksum                  uint32 `json:"checksum"`
}

type JournalManifestInspection struct {
	Present bool   `json:"present"`
	State   string `json:"state,omitempty"`
}

// Journal durably appends standalone transaction coordinator snapshots.
type Journal struct {
	mu                        sync.Mutex
	path                      string
	validEnd                  int64
	loaded                    bool
	latest                    map[string]*Snapshot
	latestBytes               int64
	latestRecordBytes         map[string]int64
	records                   int
	nextProducerEpoch         uint64
	hasProducerEpochWatermark bool
}

var syncJournalCreationDirectory = syncJournalDirectory

func OpenJournal(path string) (*Journal, error) {
	if path == "" {
		return nil, fmt.Errorf("transaction journal path is empty")
	}
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	_, statErr := os.Stat(dir)
	dirCreated := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !dirCreated {
		return nil, fmt.Errorf("stat transaction journal directory: %w", statErr)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create transaction journal directory: %w", err)
	}

	// O_EXCL tells us whether this call introduced the authoritative filename.
	// Existing journals are opened without O_CREATE so later append/recovery
	// paths cannot silently recreate a deleted, unsynced journal.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	fileCreated := err == nil
	if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("open transaction journal: %w", err)
	}
	if fileCreated {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("sync new transaction journal: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close transaction journal: %w", err)
	}
	// Sync on every open so journals created by an older release also acquire a
	// durable directory entry before their first post-upgrade ACK.
	if err := syncJournalCreationDirectory(dir); err != nil {
		if fileCreated {
			_ = os.Remove(path)
		}
		return nil, fmt.Errorf("persist transaction journal entry: %w", err)
	}
	if dirCreated {
		if err := syncJournalCreationDirectory(filepath.Dir(dir)); err != nil {
			if fileCreated {
				_ = os.Remove(path)
				_ = os.Remove(dir)
			}
			return nil, fmt.Errorf("persist transaction journal directory: %w", err)
		}
	}
	journal := &Journal{path: path}
	if _, err := journal.Load(); err != nil {
		return nil, fmt.Errorf("recover transaction journal: %w", err)
	}
	journal.mu.Lock()
	err = journal.writeManifestLocked()
	journal.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("write transaction journal manifest: %w", err)
	}
	return journal, nil
}

// InspectJournal validates an existing journal without creating, truncating, or
// repairing it. A missing journal is valid because no transaction may have
// been persisted yet.
func InspectJournal(path string) (JournalInspection, error) {
	if path == "" {
		return JournalInspection{}, fmt.Errorf("transaction journal path is empty")
	}
	file, info, missing, err := openJournalForInspection(path)
	if missing {
		return JournalInspection{}, nil
	}
	if err != nil {
		return JournalInspection{}, fmt.Errorf("open transaction journal for inspection: %w", err)
	}
	defer func() { _ = file.Close() }()

	latest := make(map[string]*Snapshot)
	var offset int64
	records := 0
	var nextProducerEpoch uint64
	hasProducerEpochWatermark := false
	for offset < info.Size() {
		if info.Size()-offset < journalRecordOverhead {
			return JournalInspection{}, fmt.Errorf("truncated transaction journal record at %d", offset)
		}
		var header [4]byte
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return JournalInspection{}, fmt.Errorf("read transaction journal header at %d: %w", offset, err)
		}
		payloadSize := int64(binary.BigEndian.Uint32(header[:]))
		if payloadSize <= 0 || payloadSize > maxJournalRecordBytes {
			return JournalInspection{}, fmt.Errorf("invalid transaction journal record size %d at %d", payloadSize, offset)
		}
		recordEnd := offset + journalRecordSize(int(payloadSize))
		if recordEnd > info.Size() {
			return JournalInspection{}, fmt.Errorf("truncated transaction journal record at %d", offset)
		}
		payload := make([]byte, payloadSize)
		if _, err := file.ReadAt(payload, offset+4); err != nil {
			return JournalInspection{}, fmt.Errorf("read transaction journal payload at %d: %w", offset, err)
		}
		var checksumBytes [4]byte
		if _, err := file.ReadAt(checksumBytes[:], offset+4+payloadSize); err != nil {
			return JournalInspection{}, fmt.Errorf("read transaction journal checksum at %d: %w", offset, err)
		}
		if actual, expected := crc32.ChecksumIEEE(payload), binary.BigEndian.Uint32(checksumBytes[:]); actual != expected {
			return JournalInspection{}, fmt.Errorf("transaction journal checksum mismatch at %d", offset)
		}
		snapshot, nextEpoch, err := decodeJournalRecord(payload)
		if err != nil {
			return JournalInspection{}, fmt.Errorf("decode transaction journal record at %d: %w", offset, err)
		}
		if snapshot != nil {
			if err := mergeJournalSnapshot(latest, snapshot); err != nil {
				return JournalInspection{}, fmt.Errorf("merge transaction journal record at %d: %w", offset, err)
			}
		}
		if snapshot == nil {
			hasProducerEpochWatermark = true
		}
		nextProducerEpoch = max(nextProducerEpoch, nextEpoch)
		offset = recordEnd
		records++
	}
	return JournalInspection{
		Present: true, RecordCount: records, LatestTransactions: len(latest),
		NextProducerEpoch: nextProducerEpoch, HasProducerEpochWatermark: hasProducerEpochWatermark,
	}, nil
}

func InspectJournalManifest(journalPath string, journal JournalInspection) (JournalManifestInspection, error) {
	manifestPath := strings.TrimSuffix(journalPath, filepath.Ext(journalPath)) + ".manifest"
	data, err := os.ReadFile(manifestPath) // #nosec G304 -- derived from the configured broker journal path.
	if errors.Is(err, os.ErrNotExist) {
		if journal.Present {
			return JournalManifestInspection{}, fmt.Errorf("transaction journal manifest is missing")
		}
		return JournalManifestInspection{}, nil
	}
	if err != nil {
		return JournalManifestInspection{}, fmt.Errorf("read transaction journal manifest: %w", err)
	}
	var manifest journalManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return JournalManifestInspection{}, fmt.Errorf("decode transaction journal manifest: %w", err)
	}
	checksum := manifest.Checksum
	manifest.Checksum = 0
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return JournalManifestInspection{}, err
	}
	if checksum != crc32.Checksum(encoded, crc32.MakeTable(crc32.Castagnoli)) {
		return JournalManifestInspection{}, fmt.Errorf("transaction journal manifest checksum mismatch")
	}
	info, err := os.Stat(journalPath)
	if err != nil {
		return JournalManifestInspection{}, fmt.Errorf("stat transaction journal for manifest: %w", err)
	}
	if manifest.Version != 1 || manifest.JournalSize != info.Size() ||
		manifest.RecordCount != journal.RecordCount || manifest.LatestTransactions != journal.LatestTransactions ||
		manifest.NextProducerEpoch != journal.NextProducerEpoch || manifest.HasProducerEpochWatermark != journal.HasProducerEpochWatermark {
		return JournalManifestInspection{}, fmt.Errorf("transaction journal manifest is stale or does not match the journal cut")
	}
	wantState := "active"
	if journal.LatestTransactions == 0 && journal.NextProducerEpoch == 0 {
		wantState = "unused"
	}
	if manifest.State != wantState {
		return JournalManifestInspection{}, fmt.Errorf("transaction journal manifest state %q does not match %q", manifest.State, wantState)
	}
	return JournalManifestInspection{Present: true, State: manifest.State}, nil
}

func (j *Journal) Append(snap *Snapshot) (err error) {
	if snap == nil || snap.ID == "" {
		return fmt.Errorf("invalid transaction snapshot")
	}
	if snap.Epoch < 0 {
		return fmt.Errorf("invalid producer epoch %d", snap.Epoch)
	}
	payload, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
	if err != nil {
		return fmt.Errorf("marshal transaction snapshot: %w", err)
	}
	payloadLen := len(payload)
	if payloadLen == 0 || payloadLen > maxJournalRecordBytes {
		return fmt.Errorf("transaction snapshot size %d exceeds journal limit", payloadLen)
	}

	var header [4]byte
	payloadSize := uint32(payloadLen) // #nosec G115 -- bounded by maxJournalRecordBytes above.
	binary.BigEndian.PutUint32(header[:], payloadSize)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))

	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.loaded {
		if _, err := j.loadLocked(); err != nil {
			return fmt.Errorf("recover transaction journal before append: %w", err)
		}
	}
	if j.shouldCompactLocked() {
		if err := j.compactLocked(); err != nil {
			return fmt.Errorf("compact transaction journal: %w", err)
		}
	}

	file, err := os.OpenFile(j.path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open transaction journal for append: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	if err := file.Truncate(j.validEnd); err != nil {
		return fmt.Errorf("truncate unacknowledged transaction journal tail: %w", err)
	}
	if _, err := file.Seek(j.validEnd, io.SeekStart); err != nil {
		return fmt.Errorf("seek transaction journal append position: %w", err)
	}
	if err := writeFull(file, header[:]); err != nil {
		return fmt.Errorf("append transaction journal header: %w", err)
	}
	if err := writeFull(file, payload); err != nil {
		return fmt.Errorf("append transaction journal payload: %w", err)
	}
	if err := writeFull(file, checksum[:]); err != nil {
		return fmt.Errorf("append transaction journal checksum: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync transaction journal: %w", err)
	}
	recordBytes := journalRecordSize(payloadLen)
	j.validEnd += recordBytes
	j.replaceLatestLocked(snap, recordBytes)
	j.nextProducerEpoch = max(j.nextProducerEpoch, uint64(snap.Epoch)+1)
	j.records++
	return j.writeManifestLocked()
}

func (j *Journal) shouldCompactLocked() bool {
	return j.records-len(j.latest) >= journalCompactionRecords ||
		j.validEnd-j.latestBytes >= journalCompactionBytes
}

// Rewrite atomically replaces the journal with the supplied authoritative
// transaction state. Callers use this after pruning expired tombstones so a
// removed transactional ID cannot be resurrected during restart recovery.
func (j *Journal) Rewrite(state map[string]*Snapshot) error {
	return j.RewriteWithProducerEpoch(state, 0)
}

// RewriteWithProducerEpoch also records an allocator floor recovered from
// retained partition data during migration of a pre-watermark journal.
func (j *Journal) RewriteWithProducerEpoch(state map[string]*Snapshot, nextEpoch uint64) error {
	if err := ValidateProducerEpochWatermark(nextEpoch); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.loaded {
		if _, err := j.loadLocked(); err != nil {
			return fmt.Errorf("recover transaction journal before rewrite: %w", err)
		}
	}
	j.nextProducerEpoch = max(j.nextProducerEpoch, nextEpoch)

	next := make(map[string]*Snapshot, len(state))
	for id, snap := range state {
		if snap == nil || id == "" || snap.ID != id {
			return fmt.Errorf("invalid transaction snapshot for %q during rewrite", id)
		}
		if snap.Epoch < 0 {
			return fmt.Errorf("invalid producer epoch %d", snap.Epoch)
		}
		next[id] = snapshot(transactionFromSnapshot(snap))
		j.nextProducerEpoch = max(j.nextProducerEpoch, uint64(snap.Epoch)+1)
	}
	j.latest = next
	j.latestBytes = 0
	j.latestRecordBytes = make(map[string]int64, len(next))
	if err := j.compactLocked(); err != nil {
		return fmt.Errorf("rewrite transaction journal: %w", err)
	}
	return nil
}

func (j *Journal) compactLocked() (err error) {
	dir := filepath.Dir(j.path)
	temp, err := os.CreateTemp(dir, filepath.Base(j.path)+".compact-*")
	if err != nil {
		return fmt.Errorf("create compacted transaction journal: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		if temp != nil {
			err = errors.Join(err, temp.Close())
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove transaction journal temp file: %w", removeErr))
		}
	}()

	ids := make([]string, 0, len(j.latest))
	for id := range j.latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	// The watermark precedes transaction records and survives an empty rewrite.
	// The whole compacted file is synced and atomically installed together.
	compactedSize, err := writeJournalRecord(temp, journalRecord{Version: journalFormatVersion, NextProducerEpoch: &j.nextProducerEpoch})
	if err != nil {
		return fmt.Errorf("write producer epoch watermark: %w", err)
	}
	compactedRecordBytes := make(map[string]int64, len(ids))
	for _, id := range ids {
		snap := j.latest[id]
		if snap == nil || snap.ID == "" {
			return fmt.Errorf("invalid transaction snapshot for %q during compaction", id)
		}
		payload, marshalErr := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
		if marshalErr != nil {
			return fmt.Errorf("marshal transaction snapshot %q during compaction: %w", id, marshalErr)
		}
		if len(payload) == 0 || len(payload) > maxJournalRecordBytes {
			return fmt.Errorf("transaction snapshot %q size %d exceeds journal limit", id, len(payload))
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload))) // #nosec G115 -- bounded above.
		var checksum [4]byte
		binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
		if writeErr := writeFull(temp, header[:]); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal header: %w", writeErr)
		}
		if writeErr := writeFull(temp, payload); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal payload: %w", writeErr)
		}
		if writeErr := writeFull(temp, checksum[:]); writeErr != nil {
			return fmt.Errorf("write compacted transaction journal checksum: %w", writeErr)
		}
		recordBytes := journalRecordSize(len(payload))
		compactedSize += recordBytes
		compactedRecordBytes[id] = recordBytes
	}
	if syncErr := temp.Sync(); syncErr != nil {
		return fmt.Errorf("sync compacted transaction journal: %w", syncErr)
	}
	if closeErr := temp.Close(); closeErr != nil {
		return fmt.Errorf("close compacted transaction journal: %w", closeErr)
	}
	temp = nil
	if renameErr := os.Rename(tempPath, j.path); renameErr != nil {
		return fmt.Errorf("replace transaction journal with compacted state: %w", renameErr)
	}
	j.validEnd = compactedSize
	j.latestBytes = compactedSize
	j.latestRecordBytes = compactedRecordBytes
	j.records = len(ids) + 1
	j.hasProducerEpochWatermark = true
	if syncErr := syncJournalDirectory(dir); syncErr != nil {
		return syncErr
	}
	return j.writeManifestLocked()
}

func (j *Journal) writeManifestLocked() error {
	info, err := os.Stat(j.path)
	if err != nil {
		return fmt.Errorf("stat transaction journal: %w", err)
	}
	state := "active"
	if len(j.latest) == 0 && j.nextProducerEpoch == 0 {
		state = "unused"
	}
	manifest := journalManifest{
		Version: 1, State: state, JournalSize: info.Size(), RecordCount: j.records,
		LatestTransactions: len(j.latest), NextProducerEpoch: j.nextProducerEpoch,
		HasProducerEpochWatermark: j.hasProducerEpochWatermark,
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	manifest.Checksum = crc32.Checksum(encoded, crc32.MakeTable(crc32.Castagnoli))
	encoded, err = json.Marshal(manifest)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	manifestPath := strings.TrimSuffix(j.path, filepath.Ext(j.path)) + ".manifest"
	file, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- derived from broker-owned journal path.
	if err != nil {
		return fmt.Errorf("open transaction journal manifest: %w", err)
	}
	if err := writeFull(file, encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("write transaction journal manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync transaction journal manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close transaction journal manifest: %w", err)
	}
	if err := syncJournalDirectory(filepath.Dir(manifestPath)); err != nil {
		return fmt.Errorf("persist transaction journal manifest: %w", err)
	}
	return nil
}

func (j *Journal) Load() (map[string]*Snapshot, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.loadLocked()
}

func (j *Journal) loadLocked() (map[string]*Snapshot, error) {
	j.loaded = false
	j.hasProducerEpochWatermark = false
	file, err := os.OpenFile(j.path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction journal for recovery: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat transaction journal: %w", err)
	}
	size := info.Size()
	j.nextProducerEpoch = 0
	latest := make(map[string]*Snapshot)
	latestRecordBytes := make(map[string]int64)
	var latestBytes int64
	var offset int64
	records := 0

	for offset < size {
		if size-offset < 4 {
			return j.repairTail(file, offset, latest, latestRecordBytes, latestBytes, records)
		}

		var header [4]byte
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return nil, fmt.Errorf("read transaction journal header at %d: %w", offset, err)
		}
		payloadSize := int64(binary.BigEndian.Uint32(header[:]))
		if payloadSize <= 0 || payloadSize > maxJournalRecordBytes {
			if offset+4 == size {
				return j.repairTail(file, offset, latest, latestRecordBytes, latestBytes, records)
			}
			return nil, fmt.Errorf("invalid transaction journal record size %d at %d", payloadSize, offset)
		}

		recordEnd := offset + 4 + payloadSize + 4
		if recordEnd > size {
			return j.repairTail(file, offset, latest, latestRecordBytes, latestBytes, records)
		}

		payload := make([]byte, payloadSize)
		if _, err := file.ReadAt(payload, offset+4); err != nil {
			return nil, fmt.Errorf("read transaction journal payload at %d: %w", offset, err)
		}
		var checksumBytes [4]byte
		if _, err := file.ReadAt(checksumBytes[:], offset+4+payloadSize); err != nil {
			return nil, fmt.Errorf("read transaction journal checksum at %d: %w", offset, err)
		}
		expected := binary.BigEndian.Uint32(checksumBytes[:])
		actual := crc32.ChecksumIEEE(payload)
		if actual != expected {
			// A complete record may be an acknowledged transaction or the sole
			// epoch watermark after compaction. Never discard it as an
			// unacknowledged tail: doing so could reuse a persisted identity.
			return nil, fmt.Errorf("transaction journal checksum mismatch at %d", offset)
		}

		snap, nextEpoch, err := decodeJournalRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("decode transaction journal record at %d: %w", offset, err)
		}
		j.nextProducerEpoch = max(j.nextProducerEpoch, nextEpoch)
		if snap != nil {
			if err := mergeJournalSnapshot(latest, snap); err != nil {
				return nil, fmt.Errorf("merge transaction journal record at %d: %w", offset, err)
			}
			if previousSize, exists := latestRecordBytes[snap.ID]; exists {
				latestBytes -= previousSize
			}
			recordBytes := recordEnd - offset
			latestRecordBytes[snap.ID] = recordBytes
			latestBytes += recordBytes
		} else {
			latestBytes += recordEnd - offset
			j.hasProducerEpochWatermark = true
		}
		records++
		offset = recordEnd
	}
	j.validEnd = offset
	j.loaded = true
	j.latest = latest
	j.latestBytes = latestBytes
	j.latestRecordBytes = latestRecordBytes
	j.records = records
	return cloneJournalState(latest), nil
}

func (j *Journal) repairTail(file *os.File, offset int64, latest map[string]*Snapshot, latestRecordBytes map[string]int64, latestBytes int64, records int) (map[string]*Snapshot, error) {
	if offset == 0 {
		return nil, fmt.Errorf("incomplete first transaction journal record; cannot safely recover producer epoch watermark")
	}
	if err := repairJournalTail(file, offset); err != nil {
		return nil, err
	}
	j.validEnd = offset
	j.loaded = true
	j.latest = latest
	j.latestBytes = latestBytes
	j.latestRecordBytes = latestRecordBytes
	j.records = records
	return cloneJournalState(latest), nil
}

func (j *Journal) replaceLatestLocked(snap *Snapshot, recordBytes int64) {
	if previousSize, exists := j.latestRecordBytes[snap.ID]; exists {
		j.latestBytes -= previousSize
	}
	j.latest[snap.ID] = snapshot(transactionFromSnapshot(snap))
	j.latestRecordBytes[snap.ID] = recordBytes
	j.latestBytes += recordBytes
}

func journalRecordSize(payloadLen int) int64 {
	return int64(journalRecordOverhead) + int64(payloadLen)
}

func cloneJournalState(state map[string]*Snapshot) map[string]*Snapshot {
	cloned := make(map[string]*Snapshot, len(state))
	for id, snap := range state {
		if snap == nil {
			continue
		}
		copySnapshot := *snap
		copySnapshot.Messages = append([]MessageOperation(nil), snap.Messages...)
		for i := range copySnapshot.Messages {
			message := &copySnapshot.Messages[i].Message
			message.ControlBatchKey = append([]byte(nil), message.ControlBatchKey...)
			message.ControlBatchValue = append([]byte(nil), message.ControlBatchValue...)
		}
		copySnapshot.Offsets = append([]OffsetOperation(nil), snap.Offsets...)
		cloned[id] = &copySnapshot
	}
	return cloned
}

func decodeJournalSnapshot(payload []byte) (*Snapshot, error) {
	snap, _, err := decodeJournalRecord(payload)
	return snap, err
}

func decodeJournalRecord(payload []byte) (*Snapshot, uint64, error) {
	var record journalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, 0, err
	}
	if record.Version != 1 && record.Version != journalFormatVersion {
		return nil, 0, fmt.Errorf("unsupported transaction journal version %d", record.Version)
	}
	if record.NextProducerEpoch != nil {
		if record.Version != journalFormatVersion || record.Transaction != nil {
			return nil, 0, fmt.Errorf("invalid producer epoch watermark record")
		}
		if err := ValidateProducerEpochWatermark(*record.NextProducerEpoch); err != nil {
			return nil, 0, err
		}
		return nil, *record.NextProducerEpoch, nil
	}
	if record.Transaction == nil || record.Transaction.ID == "" {
		return nil, 0, fmt.Errorf("journal transaction is missing")
	}
	if record.Transaction.Epoch < 0 {
		return nil, 0, fmt.Errorf("invalid producer epoch %d", record.Transaction.Epoch)
	}
	return record.Transaction, uint64(record.Transaction.Epoch) + 1, nil
}

// NextProducerEpoch is recovered by Load, including from a journal with no
// remaining transactions after retention. Callers restore it before serving.
func (j *Journal) NextProducerEpoch() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.nextProducerEpoch
}

func (j *Journal) HasProducerEpochWatermark() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.hasProducerEpochWatermark
}

func writeJournalRecord(w io.Writer, record journalRecord) (int64, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	if len(payload) == 0 || len(payload) > maxJournalRecordBytes {
		return 0, fmt.Errorf("journal record exceeds size limit")
	}
	var header, checksum [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload))) // #nosec G115 -- bounded above.
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
	for _, part := range [][]byte{header[:], payload, checksum[:]} {
		if err := writeFull(w, part); err != nil {
			return 0, err
		}
	}
	return journalRecordSize(len(payload)), nil
}

func mergeJournalSnapshot(latest map[string]*Snapshot, incoming *Snapshot) error {
	// Per-transaction controller locks serialize journal appends. The final
	// record is authoritative; epoch allocation remains monotonic even when
	// retention removes an ID's previous revision metadata.
	latest[incoming.ID] = incoming
	return nil
}

func repairJournalTail(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate incomplete transaction journal tail: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync repaired transaction journal: %w", err)
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
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
