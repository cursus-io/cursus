package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
)

const bootstrapManifestMaxBytes = 2 << 20

type bootstrapInstallFile struct {
	BootstrapFile
	Destination string `json:"destination"`
}
type bootstrapTransaction struct {
	Version  int                    `json:"version"`
	Metadata BootstrapMetadata      `json:"metadata"`
	StageID  string                 `json:"stage_id"`
	Files    []bootstrapInstallFile `json:"files"`
	Delete   []string               `json:"delete"`
}
type bootstrapEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

// Fault seam runs after each durable-file replacement, including during replay.
// Tests set it only while no other install/recovery is running.
var bootstrapAfterReplace func(string) error

// Post-commit garbage collection is deliberately outside the success boundary.
// Tests inject failures here to prove a durable completion is not reported failed.
var bootstrapCleanupCompleted = func(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncBootstrapDirectory(filepath.Dir(dir))
}

func bootstrapPendingPath(base string) string   { return base + ".bootstrap.pending" }
func bootstrapCompletedPath(base string) string { return base + ".bootstrap.completed" }

func writeBootstrapEnvelope(path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(bootstrapEnvelope{Payload: payload, SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if len(data) > bootstrapManifestMaxBytes {
		return fmt.Errorf("bootstrap manifest exceeds size bound")
	}
	temp := path + ".tmp"
	if err = os.Remove(temp); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = writeBootstrapFile(temp, data); err != nil {
		return err
	}
	if err = replaceCompactedFile(temp, path); err != nil {
		return err
	}
	return syncBootstrapDirectory(filepath.Dir(path))
}

func readBootstrapEnvelope(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > bootstrapManifestMaxBytes {
		return fmt.Errorf("invalid bootstrap manifest file")
	}
	// #nosec G304 -- callers use partition-owned pending/completed manifest paths; Lstat above rejects nonregular files.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var envelope bootstrapEnvelope
	if err = strictBootstrapJSON(data, &envelope); err != nil {
		return err
	}
	sum := sha256.Sum256(envelope.Payload)
	if hex.EncodeToString(sum[:]) != envelope.SHA256 {
		return fmt.Errorf("bootstrap manifest checksum mismatch")
	}
	return strictBootstrapJSON(envelope.Payload, value)
}
func strictBootstrapJSON(data []byte, value any) error {
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("bootstrap manifest contains trailing data")
	}
	return nil
}

// bootstrapOwnedName accepts only recognized exact partition-owned files.
// Event-source files live inside the partition directory, not beside log files.
func bootstrapOwnedName(base, name string) bool {
	leaf := filepath.Base(base)
	if filepath.IsAbs(name) || strings.Contains(name, "\\") || strings.Contains(name, "..") {
		return false
	}
	for _, suffix := range []string{".hwm", ".hwm.tmp", ".producers", ".producers.tmp", ".lineage", "_compacted_range.pending", "_compacted_range.pending.compacting"} {
		if name == leaf+suffix {
			return true
		}
	}
	for _, suffix := range []string{"_stream.idx", "_stream_keys.dat", "_snapshots.dat", "_snapshots_v2.dat"} {
		if name == leaf+"/"+leaf+suffix || name == leaf+suffix {
			return true
		}
	}
	pattern := `^` + regexp.QuoteMeta(leaf) + `_segment_[0-9]{20}\.(log|index)(\.compacted-[0-9]+)?(\.deleted|\.compacting|\.range\.compacting)?$`
	return regexp.MustCompile(pattern).MatchString(name)
}

func bootstrapInventory(base string) ([]string, error) {
	root := filepath.Dir(base)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if bootstrapOwnedName(base, name) {
			if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
				return nil, fmt.Errorf("nonregular bootstrap-owned file")
			}
			names = append(names, name)
		}
	}
	leaf := filepath.Base(base)
	info, err := os.Lstat(base)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("invalid event-source partition directory")
		}
		entries, err = os.ReadDir(base)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			name := leaf + "/" + entry.Name()
			if bootstrapOwnedName(base, name) {
				if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
					return nil, fmt.Errorf("nonregular event-source file")
				}
				names = append(names, name)
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if len(names) > 10000 {
		return nil, fmt.Errorf("bootstrap owned inventory exceeds bound")
	}
	sort.Strings(names)
	return names, nil
}

func bootstrapSafePath(base, name string) (string, error) {
	if !bootstrapOwnedName(base, name) {
		return "", fmt.Errorf("invalid partition-owned bootstrap destination")
	}
	root := filepath.Dir(base)
	path := filepath.Join(root, filepath.FromSlash(name))
	if strings.Contains(name, "/") {
		info, err := os.Lstat(base)
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("invalid bootstrap subdirectory")
		}
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("nonregular bootstrap destination")
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return path, nil
}

func validateBootstrapTransaction(base string, tx bootstrapTransaction) error {
	if tx.Version != BootstrapVersion || !validBootstrapID(tx.StageID) {
		return fmt.Errorf("invalid pending bootstrap version or staging ID")
	}
	if _, err := validateBootstrapMetadata(tx.Metadata); err != nil {
		return err
	}
	m := tx.Metadata
	if filepath.Base(base) != fmt.Sprintf("partition_%d", m.Fence.Partition) || filepath.Base(filepath.Dir(base)) != m.Fence.Topic {
		return fmt.Errorf("pending bootstrap destination identity mismatch")
	}
	if len(tx.Files) < 2 || len(tx.Files) > 5 || len(tx.Delete) > 10000 {
		return fmt.Errorf("invalid bootstrap file inventory")
	}
	d := &DiskHandler{BaseName: base}
	expected := map[string]string{"records": filepath.Base(d.GetSegmentPath(m.StartOffset)), "range-index": filepath.Base(d.GetIndexPath(m.StartOffset))}
	if m.StartOffset < m.EndOffset {
		if m.Compacted {
			expected["marker"] = filepath.Base(compactionMarkerPath(d.GetSegmentPath(m.StartOffset), m.Files[0].Size))
		}
		expected["tail-log"] = filepath.Base(d.GetSegmentPath(m.EndOffset))
		expected["tail-index"] = filepath.Base(d.GetIndexPath(m.EndOffset))
	}
	if len(expected) != len(tx.Files) {
		return fmt.Errorf("incomplete bootstrap installation inventory")
	}
	for _, f := range tx.Files {
		dest, ok := expected[f.ID]
		if !ok || dest != f.Destination || f.Role != "local-install" || f.Size < 0 || f.Size > bootstrapStructuralMaxBytes {
			return fmt.Errorf("invalid bootstrap replacement")
		}
		sum, err := hex.DecodeString(f.SHA256)
		if err != nil || len(sum) != sha256.Size {
			return fmt.Errorf("invalid bootstrap replacement checksum")
		}
		if f.ID == "records" && (f.Size != m.Files[0].Size || f.SHA256 != m.Files[0].SHA256) {
			return fmt.Errorf("bootstrap records descriptor conflict")
		}
		delete(expected, f.ID)
	}
	seen := map[string]bool{}
	for _, name := range tx.Delete {
		if !bootstrapOwnedName(base, name) || seen[name] {
			return fmt.Errorf("invalid bootstrap deletion inventory")
		}
		seen[name] = true
		for _, f := range tx.Files {
			if f.Destination == name {
				return fmt.Errorf("bootstrap deletion overlaps installation")
			}
		}
	}
	return nil
}

func stageBootstrapIndex(ctx context.Context, d *DiskHandler, t *bootstrapTransfer) ([]bootstrapInstallFile, error) {
	m := t.metadata
	if err := buildBootstrapCanonicalIndex(ctx, t, d.indexInterval, d.IndexSize); err != nil {
		return nil, err
	}
	var err error
	names := []struct{ id, dest string }{{"records", filepath.Base(d.GetSegmentPath(m.StartOffset))}, {"range-index", filepath.Base(d.GetIndexPath(m.StartOffset))}}
	if m.StartOffset < m.EndOffset {
		if m.Compacted {
			if err = writeBootstrapFile(filepath.Join(t.dir, "marker"), []byte(compactionMarkerVersion)); err != nil {
				return nil, err
			}
		}
		if err = writeBootstrapFile(filepath.Join(t.dir, "tail-log"), nil); err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(filepath.Join(t.dir, "tail-index"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		// #nosec G115 -- StageBootstrap admitted IndexSize <= MaxTransferBytes <= bootstrapStructuralMaxBytes before creating this transfer.
		if err = f.Truncate(int64(d.IndexSize)); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err = f.Close(); err != nil {
			return nil, err
		}
		if m.Compacted {
			names = append(names, struct{ id, dest string }{"marker", filepath.Base(compactionMarkerPath(d.GetSegmentPath(m.StartOffset), m.Files[0].Size))})
		}
		names = append(names, struct{ id, dest string }{"tail-log", filepath.Base(d.GetSegmentPath(m.EndOffset))}, struct{ id, dest string }{"tail-index", filepath.Base(d.GetIndexPath(m.EndOffset))})
	}
	var files []bootstrapInstallFile
	for _, name := range names {
		f, err := describeBootstrapFileContext(ctx, filepath.Join(t.dir, name.id), name.id, "local-install")
		if err != nil {
			return nil, err
		}
		files = append(files, bootstrapInstallFile{BootstrapFile: f, Destination: name.dest})
	}
	return files, nil
}

// PrepareContext verifies incoming bytes and builds the local installation files
// without acquiring live partition gates or writing a durable installation intent.
// Success seals chunk writes; repeated preparation does not rebuild files. The
// original staging TTL and Abort ownership remain in effect. A failed attempt
// retains its reservation and partial files for retry or Abort.
// Cancellation is checked between records, checksum chunks and preparation steps;
// mutex waits and individual filesystem calls, including fsync, are synchronous.
func (r *BootstrapReceiver) PrepareContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("bootstrap preparation context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t := r.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	return r.prepareLocked(ctx)
}

// The caller owns the transfer mutex throughout preparation and publication.
func (r *BootstrapReceiver) prepareLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := r.transfer
	if t.installed {
		if t.installFinished {
			return nil
		}
		return fmt.Errorf("bootstrap replacement requires restart recovery")
	}
	if t.closed {
		return fmt.Errorf("bootstrap transfer expired or closed")
	}
	if len(t.preparedFiles) != 0 {
		return nil
	}
	if err := verifyBootstrapIncoming(ctx, t); err != nil {
		return err
	}
	// A pre-intent failure may leave locally generated files. Regenerate these
	// on retry without touching the verified incoming records/catalog. Once a
	// durable pending manifest exists, the installed guard above forbids this.
	for _, id := range []string{"range-index", "marker", "tail-log", "tail-index"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(t.dir, id)
		if info, err := os.Lstat(path); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("nonregular generated bootstrap file")
			}
			if err = os.Remove(path); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	files, err := stageBootstrapIndex(ctx, r.handler, t)
	if err != nil {
		return err
	}
	if err = syncBootstrapDirectory(t.dir); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	t.preparedFiles = files
	return nil
}

// Install commits replacement but deliberately leaves the partition pending.
// The upper layer must have closed derived file handles before entering it.
// An earlier PrepareContext avoids index construction here, but installation
// still revalidates staged bytes before intent and verifies replay checksums.
func (r *BootstrapReceiver) Install() error {
	t := r.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	d := r.handler
	prepared := len(t.preparedFiles) != 0
	if err := r.prepareLocked(context.Background()); err != nil {
		return err
	}
	if t.installed {
		return nil
	}
	if prepared {
		if err := verifyBootstrapIncoming(context.Background(), t); err != nil {
			return err
		}
		for _, f := range t.preparedFiles {
			// Incoming verification already covered the identical records
			// descriptor. Generated bytes must match the sealed inventory.
			if f.ID == "records" {
				continue
			}
			if err := validateBootstrapRegularFile(filepath.Join(t.dir, f.ID), f.Size, f.SHA256); err != nil {
				return err
			}
		}
	}
	files := t.preparedFiles
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	d.Flush()
	d.replacementMu.Lock()
	defer d.replacementMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return err
	}
	old, err := bootstrapInventory(d.BaseName)
	if err != nil {
		return err
	}
	tx := bootstrapTransaction{Version: BootstrapVersion, Metadata: t.metadata, StageID: strings.TrimPrefix(t.dir, d.BaseName+".bootstrap-"), Files: files}
	for _, name := range old {
		keep := false
		for _, f := range files {
			if f.Destination == name {
				keep = true
				break
			}
		}
		if !keep {
			tx.Delete = append(tx.Delete, name)
		}
	}
	if err = validateBootstrapTransaction(d.BaseName, tx); err != nil {
		return err
	}
	d.advanceContentGenerationLocked()
	if err = writeBootstrapEnvelope(bootstrapPendingPath(d.BaseName), tx); err != nil {
		if _, statErr := os.Stat(bootstrapPendingPath(d.BaseName)); statErr == nil {
			d.bootstrapPending.Store(true)
			d.bootstrapInstalled.Store(false)
			t.installed = true
			t.timer.Stop()
			t.releaseTransientOwnership()
		}
		return err
	}
	d.bootstrapPending.Store(true)
	d.bootstrapInstalled.Store(false)
	t.installed = true
	t.timer.Stop()
	t.releaseTransientOwnership()
	if err = d.segmentReaders.invalidateAll(); err != nil {
		return err
	}
	if err = d.closeActiveSegmentForCompactedRange(); err != nil {
		return err
	}
	if err = replayBootstrapTransaction(d.BaseName, tx); err != nil {
		return err
	}
	d.lineage = nil
	d.lineageAutoSuppressed = false
	d.CurrentSegment = t.metadata.EndOffset
	d.CurrentOffset = 0
	d.lastIndexPosition = 0
	d.indexBytesWritten = 0
	d.segmentCreatedAt = time.Now()
	d.segments = []uint64{t.metadata.StartOffset}
	if t.metadata.EndOffset != t.metadata.StartOffset {
		d.segments = append(d.segments, t.metadata.EndOffset)
	}
	d.compactionMu.Lock()
	d.compactedSegments = map[uint64]int64{}
	if t.metadata.Compacted && t.metadata.StartOffset < t.metadata.EndOffset {
		d.compactedSegments[t.metadata.StartOffset] = t.metadata.Files[0].Size
	}
	d.compactionMu.Unlock()
	atomic.StoreUint64(&d.AbsoluteOffset, t.metadata.EndOffset)
	atomic.StoreUint64(&d.FlushedOffset, t.metadata.EndOffset)
	d.writtenTail = t.metadata.EndOffset
	if err = d.openSegment(); err != nil {
		return err
	}
	if err = d.openIndexFiles(); err != nil {
		return err
	}
	d.bootstrapInstalled.Store(true)
	t.installFinished = true
	return nil
}

func replayBootstrapTransaction(base string, tx bootstrapTransaction) error {
	if err := validateBootstrapTransaction(base, tx); err != nil {
		return err
	}
	dir := base + ".bootstrap-" + tx.StageID
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid bootstrap staging directory")
	}
	catalog := tx.Metadata.Files[1]
	if err = validateBootstrapRegularFile(filepath.Join(dir, catalog.ID), catalog.Size, catalog.SHA256); err != nil {
		return fmt.Errorf("pending bootstrap catalog: %w", err)
	}
	for _, f := range tx.Files {
		destination, err := bootstrapSafePath(base, f.Destination)
		if err != nil {
			return err
		}
		stage := filepath.Join(dir, f.ID)
		if info, err := os.Lstat(stage); err == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular staged bootstrap file")
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err = installCompactedRangeFile(stage, destination, f.Size, f.SHA256); err != nil {
			return fmt.Errorf("install bootstrap %s: %w", f.ID, err)
		}
		if bootstrapAfterReplace != nil {
			if err = bootstrapAfterReplace(f.ID); err != nil {
				return err
			}
		}
	}
	for _, name := range tx.Delete {
		path, err := bootstrapSafePath(base, name)
		if err != nil {
			return err
		}
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if info, err := os.Lstat(base); err == nil && info.IsDir() {
		if err = syncBootstrapDirectory(base); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = syncBootstrapDirectory(dir); err != nil {
		return err
	}
	return syncBootstrapDirectory(filepath.Dir(base))
}

func recoverPendingBootstrap(base string) (*bootstrapTransaction, error) {
	var tx bootstrapTransaction
	if err := readBootstrapEnvelope(bootstrapPendingPath(base), &tx); err != nil {
		if os.IsNotExist(err) {
			return nil, cleanupBootstrapOrphans(base, "")
		}
		return nil, err
	}
	if err := replayBootstrapTransaction(base, tx); err != nil {
		return nil, err
	}
	if err := cleanupBootstrapOrphans(base, tx.StageID); err != nil {
		return nil, err
	}
	return &tx, nil
}

// PendingBootstrap returns fencing evidence even after startup mechanically
// replays a replacement. It never infers cluster authority from that evidence.
func (d *DiskHandler) PendingBootstrap() (*BootstrapMetadata, error) {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	var tx bootstrapTransaction
	if err := readBootstrapEnvelope(bootstrapPendingPath(d.BaseName), &tx); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if err := validateBootstrapTransaction(d.BaseName, tx); err != nil {
		return nil, err
	}
	m := cloneBootstrapMetadata(tx.Metadata)
	return &m, nil
}

func (d *DiskHandler) PendingBootstrapCatalog() ([]byte, error) {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	var tx bootstrapTransaction
	if err := readBootstrapEnvelope(bootstrapPendingPath(d.BaseName), &tx); err != nil {
		return nil, err
	}
	if err := validateBootstrapTransaction(d.BaseName, tx); err != nil {
		return nil, err
	}
	f := tx.Metadata.Files[1]
	path := filepath.Join(d.BaseName+".bootstrap-"+tx.StageID, f.ID)
	if err := validateBootstrapRegularFile(path, f.Size, f.SHA256); err != nil {
		return nil, err
	}
	// #nosec G304 -- validateBootstrapTransaction checked the staging ID and fixed catalog ID; the regular file and checksum were verified above.
	return os.ReadFile(path)
}

// CompleteBootstrap is called only after the owner validates current fences,
// rebuilds derived state and durably publishes HWM. Repeating the same ID is safe.
func (d *DiskHandler) CompleteBootstrap(expectedID string) error {
	if !validBootstrapID(expectedID) {
		return fmt.Errorf("invalid bootstrap completion identity")
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	var tx bootstrapTransaction
	err := readBootstrapEnvelope(bootstrapPendingPath(d.BaseName), &tx)
	if os.IsNotExist(err) {
		var completed BootstrapMetadata
		if err = readBootstrapEnvelope(bootstrapCompletedPath(d.BaseName), &completed); err != nil {
			return err
		}
		if completed.TransferID != expectedID {
			return fmt.Errorf("bootstrap completion identity mismatch")
		}
		if err = syncBootstrapDirectory(filepath.Dir(d.BaseName)); err != nil {
			return err
		}
		d.bootstrapPending.Store(false)
		return nil
	}
	if err != nil {
		return err
	}
	if err = validateBootstrapTransaction(d.BaseName, tx); err != nil {
		return err
	}
	if tx.Metadata.TransferID != expectedID {
		return fmt.Errorf("bootstrap completion identity mismatch")
	}
	// Validate final authoritative bytes without replaying deletes: the owner has
	// already rebuilt the derived files that were deliberately removed on install.
	for _, f := range tx.Files {
		if f.ID == "records" || f.ID == "tail-log" {
			path, err := bootstrapSafePath(d.BaseName, f.Destination)
			if err != nil {
				return err
			}
			if err = validateCompactedRangeFile(path, f.Size, f.SHA256); err != nil {
				return err
			}
		}
	}
	if d.CurrentSegment != tx.Metadata.EndOffset || d.file == nil {
		return fmt.Errorf("bootstrap replacement requires restart recovery")
	}
	if !d.bootstrapInstalled.Load() {
		return fmt.Errorf("bootstrap replacement requires restart recovery")
	}
	catalog := tx.Metadata.Files[1]
	if err = validateBootstrapRegularFile(filepath.Join(d.BaseName+".bootstrap-"+tx.StageID, catalog.ID), catalog.Size, catalog.SHA256); err != nil {
		return err
	}
	if err = writeBootstrapEnvelope(bootstrapCompletedPath(d.BaseName), tx.Metadata); err != nil {
		return err
	}
	if err = os.Remove(bootstrapPendingPath(d.BaseName)); err != nil {
		return err
	}
	if err = syncBootstrapDirectory(filepath.Dir(d.BaseName)); err != nil {
		return err
	}
	d.bootstrapPending.Store(false)
	// All paths are generated locally and validated by validateBootstrapTransaction.
	if err = bootstrapCleanupCompleted(d.BaseName + ".bootstrap-" + tx.StageID); err != nil {
		util.Warn("bootstrap completed; deferred staging cleanup for %s: %v", d.BaseName, err)
	}
	return nil
}

func cleanupBootstrapOrphans(base, keep string) error {
	entries, err := os.ReadDir(filepath.Dir(base))
	if err != nil {
		return err
	}
	prefix := filepath.Base(base) + ".bootstrap-"
	for _, entry := range entries {
		id := strings.TrimPrefix(entry.Name(), prefix)
		if id == entry.Name() || !validBootstrapID(id) || id == keep {
			continue
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("nonregular orphan bootstrap directory")
		}
		if err = os.RemoveAll(base + ".bootstrap-" + id); err != nil {
			return err
		}
	}
	return nil
}

func validateBootstrapRegularFile(path string, size int64, checksum string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("nonregular bootstrap file")
	}
	return validateCompactedRangeFile(path, size, checksum)
}
