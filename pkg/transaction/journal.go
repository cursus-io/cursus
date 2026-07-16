package transaction

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxJournalRecordBytes = 32 << 20
	journalFormatVersion  = 1
	journalRecordOverhead = 8
)

type journalRecord struct {
	Version     int       `json:"version"`
	Transaction *Snapshot `json:"transaction"`
}

// Journal durably appends standalone transaction coordinator snapshots.
type Journal struct {
	mu       sync.Mutex
	path     string
	validEnd int64
	loaded   bool
}

func OpenJournal(path string) (*Journal, error) {
	if path == "" {
		return nil, fmt.Errorf("transaction journal path is empty")
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create transaction journal directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close transaction journal: %w", err)
	}
	return &Journal{path: path}, nil
}

func (j *Journal) Append(snap *Snapshot) (err error) {
	if snap == nil || snap.ID == "" {
		return fmt.Errorf("invalid transaction snapshot")
	}
	payload, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
	if err != nil {
		return fmt.Errorf("marshal transaction snapshot: %w", err)
	}
	payloadLen := len(payload)
	if payloadLen == 0 || payloadLen > maxJournalRecordBytes {
		return fmt.Errorf("transaction snapshot size %d exceeds journal limit", payloadLen)
	}
	if payloadLen > math.MaxInt-journalRecordOverhead {
		return fmt.Errorf("transaction snapshot size %d overflows journal frame", payloadLen)
	}

	recordSize := payloadLen + journalRecordOverhead
	record := make([]byte, recordSize)
	payloadSize := uint32(payloadLen) // #nosec G115 -- bounded by maxJournalRecordBytes above.
	binary.BigEndian.PutUint32(record[:4], payloadSize)
	copy(record[4:], payload)
	binary.BigEndian.PutUint32(record[4+payloadLen:], crc32.ChecksumIEEE(payload))

	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.loaded {
		if _, err := j.loadLocked(); err != nil {
			return fmt.Errorf("recover transaction journal before append: %w", err)
		}
	}

	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0o600)
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
	if err := writeFull(file, record); err != nil {
		return fmt.Errorf("append transaction journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync transaction journal: %w", err)
	}
	j.validEnd += int64(len(record))
	return nil
}

func (j *Journal) Load() (map[string]*Snapshot, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.loadLocked()
}

func (j *Journal) loadLocked() (map[string]*Snapshot, error) {
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction journal for recovery: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat transaction journal: %w", err)
	}
	size := info.Size()
	latest := make(map[string]*Snapshot)
	var offset int64

	for offset < size {
		if size-offset < 4 {
			return j.repairTail(file, offset, latest)
		}

		var header [4]byte
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return nil, fmt.Errorf("read transaction journal header at %d: %w", offset, err)
		}
		payloadSize := int64(binary.BigEndian.Uint32(header[:]))
		if payloadSize <= 0 || payloadSize > maxJournalRecordBytes {
			if offset+4 == size {
				return j.repairTail(file, offset, latest)
			}
			return nil, fmt.Errorf("invalid transaction journal record size %d at %d", payloadSize, offset)
		}

		recordEnd := offset + 4 + payloadSize + 4
		if recordEnd > size {
			return j.repairTail(file, offset, latest)
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
			if recordEnd == size {
				return j.repairTail(file, offset, latest)
			}
			return nil, fmt.Errorf("transaction journal checksum mismatch at %d", offset)
		}

		snap, err := decodeJournalSnapshot(payload)
		if err != nil {
			return nil, fmt.Errorf("decode transaction journal record at %d: %w", offset, err)
		}
		if err := mergeJournalSnapshot(latest, snap); err != nil {
			return nil, fmt.Errorf("merge transaction journal record at %d: %w", offset, err)
		}
		offset = recordEnd
	}
	j.validEnd = offset
	j.loaded = true
	return latest, nil
}

func (j *Journal) repairTail(file *os.File, offset int64, latest map[string]*Snapshot) (map[string]*Snapshot, error) {
	if err := repairJournalTail(file, offset); err != nil {
		return nil, err
	}
	j.validEnd = offset
	j.loaded = true
	return latest, nil
}

func decodeJournalSnapshot(payload []byte) (*Snapshot, error) {
	var record journalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, err
	}
	if record.Version == 0 && record.Transaction == nil {
		var legacy Snapshot
		if err := json.Unmarshal(payload, &legacy); err != nil {
			return nil, err
		}
		if legacy.ID == "" {
			return nil, fmt.Errorf("journal snapshot has no id")
		}
		return &legacy, nil
	}
	if record.Version != journalFormatVersion {
		return nil, fmt.Errorf("unsupported transaction journal version %d", record.Version)
	}
	if record.Transaction == nil || record.Transaction.ID == "" {
		return nil, fmt.Errorf("journal transaction is missing")
	}
	return record.Transaction, nil
}

func mergeJournalSnapshot(latest map[string]*Snapshot, incoming *Snapshot) error {
	// Per-transaction controller locks serialize journal appends. The final
	// record is authoritative even when an expired transactional ID starts a
	// fresh producer epoch with lower revision metadata.
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
