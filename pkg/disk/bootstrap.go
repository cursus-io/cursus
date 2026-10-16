package disk

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const (
	BootstrapVersion                       = 1
	BootstrapChunkBytes                    = 256 << 10
	BootstrapCatalogBytes                  = 16 << 20
	DefaultBootstrapMaxTransferBytes int64 = 8 << 30
	DefaultBootstrapMaxTotalBytes    int64 = 32 << 30
	BootstrapMaxActive                     = 8
	BootstrapTTL                           = 5 * time.Minute
)

// This format bound is independent of operational admission limits. It leaves
// ample int64 arithmetic space for derived indexes and never sizes a buffer.
const bootstrapStructuralMaxBytes int64 = math.MaxInt64 / 32

// BootstrapLimits applies to NEW transfers; replay and matching pending/completed
// retries use stable format validation. Configure the same MaxTotalBytes on all
// handlers in a broker because their reservations share one process-wide pool.
type BootstrapLimits struct {
	MaxTransferBytes int64
	MaxTotalBytes    int64
}

func DefaultBootstrapLimits() BootstrapLimits {
	return BootstrapLimits{MaxTransferBytes: DefaultBootstrapMaxTransferBytes, MaxTotalBytes: DefaultBootstrapMaxTotalBytes}
}

func validateBootstrapLimits(limits BootstrapLimits) error {
	if limits.MaxTransferBytes <= 0 || limits.MaxTransferBytes > bootstrapStructuralMaxBytes || limits.MaxTotalBytes < limits.MaxTransferBytes {
		return fmt.Errorf("bootstrap limits require 0 < transfer bytes <= %d and total bytes >= transfer bytes", bootstrapStructuralMaxBytes)
	}
	return nil
}

// SetBootstrapLimits configures admission without revoking active transfers or
// pending installations. Each new transfer captures an immutable limits snapshot.
// Broker config wiring should call this on every handler before exposing recovery.
func (d *DiskHandler) SetBootstrapLimits(limits BootstrapLimits) error {
	if err := validateBootstrapLimits(limits); err != nil {
		return err
	}
	d.bootstrapLimitsMu.Lock()
	d.bootstrapLimits = limits
	d.bootstrapLimitsMu.Unlock()
	return nil
}

func (d *DiskHandler) BootstrapLimits() BootstrapLimits {
	d.bootstrapLimitsMu.RLock()
	limits := d.bootstrapLimits
	d.bootstrapLimitsMu.RUnlock()
	if limits == (BootstrapLimits{}) {
		return DefaultBootstrapLimits()
	}
	return limits
}

func addBootstrapBytes(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, fmt.Errorf("bootstrap byte accounting overflow")
	}
	return a + b, nil
}

var ErrBootstrapPending = errors.New("retained bootstrap awaits partition recovery completion")

type BootstrapLimitError struct {
	Resource         string
	Requested, Limit int64
}

func (e *BootstrapLimitError) Error() string {
	return fmt.Sprintf("bootstrap %s requires %d, limit %d", e.Resource, e.Requested, e.Limit)
}

// BootstrapFence is evidence for the caller's cluster/lifecycle validation.
// Disk storage persists it verbatim; it cannot authorize cluster membership.
type BootstrapFence struct {
	Topic            string `json:"topic"`
	Partition        int    `json:"partition"`
	ReceiverID       string `json:"receiver_id"`
	SourceID         string `json:"source_id"`
	LeaderID         string `json:"leader_id"`
	LeaderEpoch      int    `json:"leader_epoch"`
	LifecycleEpoch   uint64 `json:"lifecycle_epoch"`
	CommittedHWM     uint64 `json:"committed_hwm"`
	RecoveryProtocol int    `json:"recovery_protocol"`
}

type BootstrapFile struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// BootstrapMetadata describes logical bounds independently of physical records.
// A zero-byte records file can represent an empty or fully compacted range.
type BootstrapMetadata struct {
	Version     int             `json:"version"`
	TransferID  string          `json:"transfer_id"`
	StartOffset uint64          `json:"start_offset"`
	EndOffset   uint64          `json:"end_offset"`
	Compacted   bool            `json:"compacted"`
	Fence       BootstrapFence  `json:"fence"`
	Files       []BootstrapFile `json:"files"`
}

var bootstrapBudget = struct {
	sync.Mutex
	active int
	bytes  int64
}{}

type bootstrapTransfer struct {
	mu              sync.Mutex
	dir             string
	metadata        BootstrapMetadata
	reserved        int64
	limits          BootstrapLimits
	timer           *time.Timer
	closed          bool
	installed       bool
	installFinished bool
	received        map[string]int64
}

type BootstrapExport struct{ transfer *bootstrapTransfer }
type BootstrapReceiver struct {
	transfer *bootstrapTransfer
	handler  *DiskHandler
}

func newBootstrapID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func validBootstrapID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}

func cloneBootstrapMetadata(m BootstrapMetadata) BootstrapMetadata {
	m.Files = append([]BootstrapFile(nil), m.Files...)
	return m
}

func newBootstrapTransfer(base string, metadata BootstrapMetadata, size int64, limits BootstrapLimits) (*bootstrapTransfer, error) {
	if err := validateBootstrapLimits(limits); err != nil {
		return nil, err
	}
	if size < 0 || size > limits.MaxTransferBytes {
		return nil, &BootstrapLimitError{Resource: "staging bytes", Requested: size, Limit: limits.MaxTransferBytes}
	}
	bootstrapBudget.Lock()
	if bootstrapBudget.active >= BootstrapMaxActive {
		requested := bootstrapBudget.active + 1
		bootstrapBudget.Unlock()
		return nil, &BootstrapLimitError{Resource: "active transfers", Requested: int64(requested), Limit: BootstrapMaxActive}
	}
	requested, err := addBootstrapBytes(bootstrapBudget.bytes, size)
	if err != nil {
		bootstrapBudget.Unlock()
		return nil, err
	}
	if requested > limits.MaxTotalBytes {
		bootstrapBudget.Unlock()
		return nil, &BootstrapLimitError{Resource: "total staging bytes", Requested: requested, Limit: limits.MaxTotalBytes}
	}
	bootstrapBudget.active++
	bootstrapBudget.bytes += size
	bootstrapBudget.Unlock()
	t := &bootstrapTransfer{metadata: cloneBootstrapMetadata(metadata), reserved: size, limits: limits, received: map[string]int64{}}
	id, err := newBootstrapID()
	if err == nil {
		t.dir = base + ".bootstrap-" + id
		err = os.Mkdir(t.dir, 0o700)
	}
	if err != nil {
		t.releaseBudget()
		return nil, err
	}
	return t, nil
}

func (t *bootstrapTransfer) startTTL() {
	t.timer = time.AfterFunc(BootstrapTTL, func() { _ = t.close() })
}
func (t *bootstrapTransfer) releaseBudget() {
	bootstrapBudget.Lock()
	bootstrapBudget.active--
	bootstrapBudget.bytes -= t.reserved
	bootstrapBudget.Unlock()
}
func (t *bootstrapTransfer) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	if t.installed {
		return ErrBootstrapPending
	}
	t.closed = true
	if t.timer != nil {
		t.timer.Stop()
	}
	t.releaseBudget()
	return os.RemoveAll(t.dir)
}

func (e *BootstrapExport) Manifest() BootstrapMetadata {
	return cloneBootstrapMetadata(e.transfer.metadata)
}
func (e *BootstrapExport) Close() error { return e.transfer.close() }

func bootstrapFile(m BootstrapMetadata, id string) (BootstrapFile, error) {
	for _, f := range m.Files {
		if f.ID == id {
			return f, nil
		}
	}
	return BootstrapFile{}, fmt.Errorf("unknown bootstrap file ID")
}

func (e *BootstrapExport) ReadChunk(fileID string, offset int64, maxBytes int) ([]byte, error) {
	t := e.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("bootstrap transfer expired or closed")
	}
	descriptor, err := bootstrapFile(t.metadata, fileID)
	if err != nil {
		return nil, err
	}
	if offset < 0 || offset > descriptor.Size || maxBytes <= 0 || maxBytes > BootstrapChunkBytes {
		return nil, fmt.Errorf("invalid bootstrap chunk bounds")
	}
	if int64(maxBytes) > descriptor.Size-offset {
		maxBytes = int(descriptor.Size - offset)
	}
	data := make([]byte, maxBytes)
	// #nosec G304 -- fileID matched an internally generated export descriptor; t.dir is the private staging directory.
	f, err := os.Open(filepath.Join(t.dir, fileID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	_, err = f.ReadAt(data, offset)
	if len(data) == 0 && errors.Is(err, io.EOF) {
		err = nil
	}
	return data, err
}

// ExportBootstrap serializes canonical frames, including control and aborted
// transaction frames. Locks protect only the immutable copy, never chunk reads.
// catalog is an opaque bounded catalog supplied by the owning partition layer.
func (d *DiskHandler) ExportBootstrap(start, end uint64, fence BootstrapFence, catalog []byte) (*BootstrapExport, error) {
	return d.ExportBootstrapContext(context.Background(), start, end, fence, catalog)
}

// ExportBootstrapContext cancels between physical records and checksum chunks.
// A failed/canceled copy removes staging and never publishes an export handle.
// Mutex acquisition and individual filesystem calls (including an in-flight
// fsync) remain synchronous; cancellation is rechecked after each lock boundary.
func (d *DiskHandler) ExportBootstrapContext(ctx context.Context, start, end uint64, fence BootstrapFence, catalog []byte) (*BootstrapExport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("bootstrap export context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(catalog) > BootstrapCatalogBytes {
		return nil, &BootstrapLimitError{Resource: "catalog bytes", Requested: int64(len(catalog)), Limit: BootstrapCatalogBytes}
	}
	limits := d.BootstrapLimits()
	id, err := newBootstrapID()
	if err != nil {
		return nil, err
	}
	m := BootstrapMetadata{Version: BootstrapVersion, TransferID: id, StartOffset: start, EndOffset: end, Fence: fence}
	if err := validateBootstrapBounds(m); err != nil {
		return nil, err
	}
	if filepath.Base(d.BaseName) != fmt.Sprintf("partition_%d", fence.Partition) || filepath.Base(filepath.Dir(d.BaseName)) != fence.Topic {
		return nil, fmt.Errorf("bootstrap source identity mismatch")
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.Flush()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.writeAvailabilityError(); err != nil {
		return nil, err
	}
	if len(d.segments) == 0 || start < d.segments[0] || end > atomic.LoadUint64(&d.FlushedOffset) {
		return nil, fmt.Errorf("bootstrap range outside durable retained bounds")
	}
	size := int64(len(catalog))
	for i, segment := range d.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if segment >= end {
			break
		}
		if i+1 < len(d.segments) && d.segments[i+1] <= start {
			continue
		}
		info, err := os.Stat(d.GetSegmentPath(segment))
		if err != nil {
			return nil, err
		}
		size, err = addBootstrapBytes(size, info.Size())
		if err != nil {
			return nil, err
		}
		if size > limits.MaxTransferBytes {
			return nil, &BootstrapLimitError{Resource: "source bytes", Requested: size, Limit: limits.MaxTransferBytes}
		}
	}
	if _, err := d.headroom.reserve(uint64(size), true); err != nil {
		return nil, err
	}
	t, err := newBootstrapTransfer(d.BaseName, m, size, limits)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = t.close()
		}
	}()
	log, err := os.OpenFile(filepath.Join(t.dir, "records"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = log.Close() }()
	var outputBytes int64
	m.Compacted, err = d.scanRetainedRangeLocked(ctx, start, end, func(msg types.DiskMessage) error {
		if msg.Topic != fence.Topic || int(msg.Partition) != fence.Partition {
			return fmt.Errorf("bootstrap record identity mismatch")
		}
		canonical, err := util.SerializeDiskMessage(msg)
		if err != nil {
			return err
		}
		if err = validateSerializedDiskMessageSize(canonical); err != nil {
			return err
		}
		length, ok := util.SafeIntToUint32(len(canonical))
		if !ok {
			return fmt.Errorf("bootstrap canonical frame too large")
		}
		outputBytes += 4 + int64(len(canonical))
		if err = reserveBootstrapGrowth(d, t, outputBytes+int64(len(catalog))); err != nil {
			return err
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], length)
		if _, err = log.Write(header[:]); err != nil {
			return err
		}
		_, err = log.Write(canonical)
		return err
	})
	if err != nil {
		return nil, err
	}

	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = d.syncFile(log); err != nil {
		return nil, err
	}
	if err = log.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = writeBootstrapFile(filepath.Join(t.dir, "catalog"), catalog); err != nil {
		return nil, err
	}
	for _, entry := range []struct{ id, role string }{{"records", "canonical-log"}, {"catalog", "snapshot-catalog"}} {
		f, err := describeBootstrapFileContext(ctx, filepath.Join(t.dir, entry.id), entry.id, entry.role)
		if err != nil {
			return nil, err
		}
		m.Files = append(m.Files, f)
	}
	if err = syncBootstrapDirectory(t.dir); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	t.metadata = m
	t.startTTL()
	ok = true
	return &BootstrapExport{transfer: t}, nil
}

func reserveBootstrapGrowth(d *DiskHandler, t *bootstrapTransfer, size int64) error {
	if size <= t.reserved {
		return nil
	}
	if size > t.limits.MaxTransferBytes {
		return &BootstrapLimitError{Resource: "canonical bytes", Requested: size, Limit: t.limits.MaxTransferBytes}
	}
	delta := size - t.reserved
	bootstrapBudget.Lock()
	defer bootstrapBudget.Unlock()
	requested, err := addBootstrapBytes(bootstrapBudget.bytes, delta)
	if err != nil {
		return err
	}
	if requested > t.limits.MaxTotalBytes {
		return &BootstrapLimitError{Resource: "total staging bytes", Requested: requested, Limit: t.limits.MaxTotalBytes}
	}
	// #nosec G115 -- size > t.reserved >= 0 above, so delta is positive and fits int64.
	if _, err := d.headroom.reserve(uint64(delta), false); err != nil {
		return err
	}
	bootstrapBudget.bytes += delta
	t.reserved = size
	return nil
}

func validateBootstrapBounds(m BootstrapMetadata) error {
	if m.Version != BootstrapVersion || !validBootstrapID(m.TransferID) || m.StartOffset > m.EndOffset || m.EndOffset > m.Fence.CommittedHWM {
		return fmt.Errorf("invalid bootstrap version, identity, or logical bounds")
	}
	f := m.Fence
	if f.Topic == "" || f.Partition < 0 || f.LeaderEpoch < 0 || f.ReceiverID == "" || f.SourceID == "" || f.LeaderID == "" || f.RecoveryProtocol <= 0 {
		return fmt.Errorf("incomplete bootstrap fencing metadata")
	}
	return nil
}

// validateBootstrapMetadata checks persistent format structure only. Never apply
// current admission limits here: a lower setting must not strand durable intent.
func validateBootstrapMetadata(m BootstrapMetadata) (int64, error) {
	if err := validateBootstrapBounds(m); err != nil {
		return 0, err
	}
	if len(m.Files) != 2 {
		return 0, fmt.Errorf("bootstrap requires records and catalog")
	}
	var total int64
	for i, expected := range []struct {
		id, role string
		limit    int64
	}{{"records", "canonical-log", bootstrapStructuralMaxBytes}, {"catalog", "snapshot-catalog", BootstrapCatalogBytes}} {
		f := m.Files[i]
		sum, err := hex.DecodeString(f.SHA256)
		if f.ID != expected.id || f.Role != expected.role || f.Size < 0 || err != nil || len(sum) != sha256.Size || hex.EncodeToString(sum) != f.SHA256 {
			return 0, fmt.Errorf("invalid bootstrap descriptor")
		}
		if f.Size > expected.limit {
			return 0, &BootstrapLimitError{Resource: f.Role + " bytes", Requested: f.Size, Limit: expected.limit}
		}
		total, err = addBootstrapBytes(total, f.Size)
		if err != nil {
			return 0, err
		}
	}
	if total > bootstrapStructuralMaxBytes {
		return 0, &BootstrapLimitError{Resource: "structural bytes", Requested: total, Limit: bootstrapStructuralMaxBytes}
	}
	return total, nil
}

// StageBootstrap accepts descriptors only, never sender-selected filesystem paths.
func (d *DiskHandler) StageBootstrap(m BootstrapMetadata) (*BootstrapReceiver, error) {
	size, err := validateBootstrapMetadata(m)
	if err != nil {
		return nil, err
	}
	if filepath.Base(d.BaseName) != fmt.Sprintf("partition_%d", m.Fence.Partition) || filepath.Base(filepath.Dir(d.BaseName)) != m.Fence.Topic {
		return nil, fmt.Errorf("bootstrap destination identity mismatch")
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	var pending bootstrapTransaction
	if err = readBootstrapEnvelope(bootstrapPendingPath(d.BaseName), &pending); err == nil {
		if err = validateBootstrapTransaction(d.BaseName, pending); err != nil {
			return nil, err
		}
		if metadataDigest(pending.Metadata) != metadataDigest(m) {
			return nil, ErrBootstrapPending
		}
		return &BootstrapReceiver{handler: d, transfer: &bootstrapTransfer{metadata: cloneBootstrapMetadata(m), dir: d.BaseName + ".bootstrap-" + pending.StageID, installed: true, installFinished: d.bootstrapInstalled.Load()}}, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	var completed BootstrapMetadata
	if err = readBootstrapEnvelope(bootstrapCompletedPath(d.BaseName), &completed); err == nil && completed.TransferID == m.TransferID {
		if metadataDigest(completed) != metadataDigest(m) {
			return nil, fmt.Errorf("conflicting completed bootstrap identity")
		}
		return &BootstrapReceiver{handler: d, transfer: &bootstrapTransfer{metadata: cloneBootstrapMetadata(m), installed: true, installFinished: true}}, nil
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err = d.writeAvailabilityError(); err != nil {
		return nil, err
	}
	// Bound all locally generated index/tail bytes before accepting a transfer.
	limits := d.BootstrapLimits()
	// #nosec G115 -- BootstrapLimits admits only positive MaxTransferBytes <= bootstrapStructuralMaxBytes.
	if d.IndexSize > uint64(limits.MaxTransferBytes) {
		return nil, &BootstrapLimitError{Resource: "tail index bytes", Requested: limits.MaxTransferBytes + 1, Limit: limits.MaxTransferBytes}
	}
	size += int64(d.IndexSize) // #nosec G115 -- the preceding guard bounds IndexSize by MaxTransferBytes <= MaxInt64/32.
	if m.StartOffset < m.EndOffset {
		interval := d.indexInterval
		if interval == 0 {
			interval = 4096
		}
		// #nosec G115 -- validateBootstrapMetadata proved the records size is nonnegative and <= MaxInt64/32.
		indexBound := (uint64(m.Files[0].Size)/interval + 1) * 16
		// #nosec G115 -- interval >= 1 and records size <= MaxInt64/32 bound indexBound below MaxInt64.
		size += int64(indexBound) + int64(len(compactionMarkerVersion))
	}
	if size > limits.MaxTransferBytes {
		return nil, &BootstrapLimitError{Resource: "staging with derived indexes", Requested: size, Limit: limits.MaxTransferBytes}
	}
	// #nosec G115 -- validated nonnegative file sizes plus bounded derived indexes fit int64; admission above caps their sum.
	if _, err = d.headroom.reserve(uint64(size), true); err != nil {
		return nil, err
	}
	t, err := newBootstrapTransfer(d.BaseName, m, size, limits)
	if err != nil {
		return nil, err
	}
	for _, f := range m.Files {
		if err = writeBootstrapFile(filepath.Join(t.dir, f.ID), nil); err != nil {
			_ = t.close()
			return nil, err
		}
	}
	t.startTTL()
	return &BootstrapReceiver{transfer: t, handler: d}, nil
}

func (r *BootstrapReceiver) Manifest() BootstrapMetadata {
	return cloneBootstrapMetadata(r.transfer.metadata)
}
func (r *BootstrapReceiver) Abort() error { return r.transfer.close() }

// WriteChunk permits exact duplicate byte ranges, but never holes or conflicts.
func (r *BootstrapReceiver) WriteChunk(fileID string, offset int64, data []byte) error {
	t := r.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("bootstrap transfer no longer accepts chunks")
	}
	f, err := bootstrapFile(t.metadata, fileID)
	if err != nil {
		return err
	}
	if offset < 0 || len(data) > BootstrapChunkBytes || int64(len(data)) > f.Size || offset > f.Size-int64(len(data)) {
		return fmt.Errorf("invalid bootstrap chunk bounds")
	}
	if t.installed {
		if !t.installFinished || t.dir == "" {
			return fmt.Errorf("bootstrap transfer no longer accepts chunks")
		}
		r.handler.replacementMu.RLock()
		defer r.handler.replacementMu.RUnlock()
		path := filepath.Join(t.dir, fileID)
		if fileID == "records" {
			path = r.handler.GetSegmentPath(t.metadata.StartOffset)
		}
		// #nosec G304 -- fileID matched a validated descriptor; path is its private staging file or this handler's generated segment path.
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		old := make([]byte, len(data))
		if _, err = file.ReadAt(old, offset); err != nil && (len(old) != 0 || !errors.Is(err, io.EOF)) {
			return err
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("conflicting duplicate bootstrap chunk")
		}
		return nil
	}
	position := t.received[fileID]
	// #nosec G304 -- fileID matched the fixed records/catalog descriptors validated by StageBootstrap; t.dir is privately generated.
	file, err := os.OpenFile(filepath.Join(t.dir, fileID), os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if offset < position {
		if int64(len(data)) > position-offset {
			return fmt.Errorf("overlapping bootstrap chunk")
		}
		old := make([]byte, len(data))
		if _, err = file.ReadAt(old, offset); err != nil {
			return err
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("conflicting duplicate bootstrap chunk")
		}
		return nil
	}
	if offset != position {
		return fmt.Errorf("out-of-order bootstrap chunk")
	}
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	t.received[fileID] += int64(n)
	return nil
}

func describeBootstrapFile(path, id, role string) (BootstrapFile, error) {
	return describeBootstrapFileContext(context.Background(), path, id, role)
}

type bootstrapContextReader struct {
	context context.Context
	reader  io.Reader
}

func (r *bootstrapContextReader) Read(data []byte) (int, error) {
	if err := r.context.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func describeBootstrapFileContext(ctx context.Context, path, id, role string) (BootstrapFile, error) {
	if err := ctx.Err(); err != nil {
		return BootstrapFile{}, err
	}
	// #nosec G304 -- callers construct path from a private staging directory and fixed local bootstrap filenames.
	f, err := os.Open(path)
	if err != nil {
		return BootstrapFile{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return BootstrapFile{}, err
	}
	if !info.Mode().IsRegular() {
		return BootstrapFile{}, fmt.Errorf("bootstrap file is not regular")
	}
	h := sha256.New()
	if _, err = io.CopyBuffer(h, &bootstrapContextReader{context: ctx, reader: f}, make([]byte, BootstrapChunkBytes)); err != nil {
		return BootstrapFile{}, err
	}
	if err = ctx.Err(); err != nil {
		return BootstrapFile{}, err
	}
	return BootstrapFile{ID: id, Role: role, Size: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func writeBootstrapFile(path string, data []byte) error {
	// #nosec G304 -- callers supply fixed staging filenames or partition-owned manifest paths; no sender-selected paths are accepted.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func metadataDigest(m BootstrapMetadata) string {
	data, _ := json.Marshal(m)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// OwnsBootstrapReceiver prevents installing a receiver belonging to another
// handler while holding this partition's visibility boundary.
func (d *DiskHandler) OwnsBootstrapReceiver(r *BootstrapReceiver) bool {
	return r != nil && r.handler == d
}
