package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// AdoptedRawStore owns a bounded cache slot and physical bytes, independently of
// active transfer slots. Closing retires it; pinned or undeleted files retain its
// slot until physical deletion succeeds.
type AdoptedRawStore struct{ transfer *bootstrapTransfer }

// DetachedAppendWork measures actual I/O, including partial work on failure.
// RawBytes and IndexBytes count destination writes; read counts cover the source
// only. No previously published prefix is read to append a prepared suffix.
type DetachedAppendWork struct {
	RawBytes, IndexBytes         uint64
	RawReadBytes, IndexReadBytes uint64
}

type DetachedAppendResult struct {
	Bounds DetachedRawBounds
	Work   DetachedAppendWork
	// A nil AppendPrepared error means publication committed, even when retiring
	// the consumed suffix failed. CleanupError must never prompt append replay.
	CleanupError error
}

type detachedTailDebt struct {
	bounds      DetachedRawBounds
	reservation int64
}

// Narrow instance seams preserve the actual files and ownership paths in tests.
type detachedAppendIO struct {
	writeAt       func(*os.File, []byte, int64) (int, error)
	sync          func(*os.File) error
	truncate      func(*os.File, int64) error
	beforePublish func() error
}

func defaultDetachedAppendIO() detachedAppendIO {
	return detachedAppendIO{
		writeAt:  func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) },
		sync:     func(f *os.File) error { return f.Sync() },
		truncate: func(f *os.File, size int64) error { return f.Truncate(size) },
	}
}

// retryAdoptedCleanup makes at most one filesystem cleanup attempt. No registry
// lock spans a child lock or I/O, and busy children do not stall admission.
func (r *DetachedBootstrapRoot) retryAdoptedCleanup() {
	r.mu.Lock()
	children := append([]*bootstrapTransfer(nil), r.adoptedOrder...)
	r.mu.Unlock()
	for _, t := range children {
		if !t.mu.TryLock() {
			continue
		}
		eligible := t.detached.retired && t.detached.leases == 0
		if eligible {
			// Rotate before the attempt, so a persistent deletion failure cannot
			// starve subsequent retired generations. Child -> root is the common
			// ordering; the registry lock is released before filesystem work.
			r.mu.Lock()
			for i, child := range r.adoptedOrder {
				if child == t {
					copy(r.adoptedOrder[i:], r.adoptedOrder[i+1:])
					r.adoptedOrder[len(r.adoptedOrder)-1] = t
					break
				}
			}
			r.mu.Unlock()
			_ = t.cleanupLocked()
		}
		t.mu.Unlock()
		if eligible {
			return
		}
	}
}

// Adopt consumes prepared ownership without copying files or releasing bytes.
// Failure leaves the prepared owner intact. Existing leases remain valid, while
// stale stage/prepared handles cannot delete or acquire the adopted generation.
func (r *DetachedBootstrapRoot) Adopt(prepared *DetachedRawStore) (*AdoptedRawStore, error) {
	r.retryAdoptedCleanup()
	if prepared == nil || prepared.transfer == nil {
		return nil, fmt.Errorf("missing detached prepared store")
	}
	t := prepared.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.detached
	if c == nil || c.root != r || !c.prepared || c.adopted || c.retired || t.closed {
		return nil, ErrDetachedBootstrapClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, ErrDetachedBootstrapClosed
	}
	if len(r.adopted) >= r.maxAdopted {
		return nil, &BootstrapLimitError{Resource: "adopted stores", Requested: int64(len(r.adopted)) + 1, Limit: int64(r.maxAdopted)}
	}
	r.adopted[t] = struct{}{}
	r.adoptedOrder = append(r.adoptedOrder, t)
	c.adopted = true
	t.activeToken.release()
	return &AdoptedRawStore{transfer: t}, nil
}

func (s *AdoptedRawStore) Acquire() (*DetachedRawLease, error) {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	return acquireDetachedLocked(t)
}

// Bounds returns the published extent without reading or hashing either file.
func (s *AdoptedRawStore) Bounds() (DetachedRawBounds, error) {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.detached.retired || t.detached.root.isClosing() {
		return DetachedRawBounds{}, ErrDetachedBootstrapClosed
	}
	return t.detached.extent, nil
}

func (s *AdoptedRawStore) Close() error {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	t.detached.retired = true
	return t.cleanupLocked()
}

// growDetached probes filesystem headroom outside the accounting mutex. The
// caller holds the private transfer mutex; admission checks and charging remain
// atomic across all active and adopted owners.
func (owner *bootstrapByteOwner) growDetached(headroom *diskHeadroomGuard, size int64) error {
	if size < 0 || !owner.owned {
		return fmt.Errorf("invalid detached byte ownership or size")
	}
	if size <= owner.reserved {
		return nil
	}
	if size > owner.limits.MaxTransferBytes {
		return &BootstrapLimitError{Resource: "canonical bytes", Requested: size, Limit: owner.limits.MaxTransferBytes}
	}
	delta := size - owner.reserved
	if _, err := headroom.reserve(uint64(delta), false); err != nil {
		return err
	}
	bootstrapBudget.Lock()
	defer bootstrapBudget.Unlock()
	requested, err := addBootstrapBytes(bootstrapBudget.bytes, delta)
	if err != nil {
		return err
	}
	if requested > owner.limits.MaxTotalBytes {
		return &BootstrapLimitError{Resource: "total staging bytes", Requested: requested, Limit: owner.limits.MaxTotalBytes}
	}
	bootstrapBudget.bytes = requested
	owner.reserved = size
	return nil
}

func (owner *bootstrapByteOwner) shrinkDetached(size int64) {
	bootstrapBudget.Lock()
	bootstrapBudget.bytes -= owner.reserved - size
	owner.reserved = size
	bootstrapBudget.Unlock()
}

func openDetachedAppendFiles(t *bootstrapTransfer) (*os.File, *os.File, error) {
	raw, err := os.OpenFile(filepath.Join(t.dir, "records"), os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	index, err := os.OpenFile(filepath.Join(t.dir, "range-index"), os.O_RDWR, 0o600)
	if err != nil {
		_ = raw.Close()
		return nil, nil, err
	}
	return raw, index, nil
}

// Both truncations and both syncs are attempted even if an earlier operation
// fails. Charges are released only once all four operations succeeded.
func repairDetachedTail(t *bootstrapTransfer, raw, index *os.File) error {
	debt := t.detached.tailDebt
	if debt == nil {
		return nil
	}
	ops := t.detached.root.appendIO
	err := errors.Join(ops.truncate(raw, debt.bounds.RawBytes), ops.truncate(index, debt.bounds.IndexBytes), ops.sync(raw), ops.sync(index))
	if err != nil {
		return err
	}
	t.byteOwner.shrinkDetached(debt.reservation)
	t.detached.tailDebt = nil
	return nil
}

// AppendPrepared appends a verified suffix to this generation. The upper layer
// must certify source continuity and fencing; disk checks identity,
// adjacency, and the entire expected extent, without equating transfer IDs.
// Validation, file-open, and reservation failures leave suffix caller-owned.
// Once copying starts, suffix is consumed and retired on every outcome. A nil
// error means the extent committed; CleanupError reports remaining suffix debt.
// Failed rollback retains peak bytes and blocks append until a retry repairs it.
func (s *AdoptedRawStore) AppendPrepared(ctx context.Context, expected DetachedRawBounds, suffix *DetachedRawStore) (result DetachedAppendResult, resultErr error) {
	if ctx == nil {
		return result, fmt.Errorf("detached append requires context")
	}
	t := s.transfer
	if suffix == nil || suffix.transfer == nil || suffix.transfer == t {
		return result, fmt.Errorf("invalid detached suffix")
	}
	unlock, err := lockProofContext(ctx, &t.mu)
	if err != nil {
		return result, err
	}
	defer unlock()
	u := suffix.transfer
	// Only adopted destinations can append; only nonadopted sources qualify.
	// Thus there is no inverse eligible pair with the opposite lock ordering.
	// TryLock also avoids waiting on an invalid adopted-source pair.
	if !u.mu.TryLock() {
		return result, ErrDetachedBootstrapPending
	}
	defer u.mu.Unlock()
	c, source := t.detached, u.detached
	if c == nil || !c.adopted || c.retired || t.closed || c.root.isClosing() {
		return result, ErrDetachedBootstrapClosed
	}
	if source == nil || source.root != c.root || !source.prepared || source.adopted || source.retired || u.closed || source.leases != 0 {
		return result, fmt.Errorf("detached suffix is not exclusively prepared in this root")
	}
	old, incoming := c.extent, source.extent
	// Compacted is each prepared range's gap authority, not a cache format.
	// Preparation already validated both ranges; the original manifest stays immutable.
	if expected != old || t.metadata.Fence.Topic != u.metadata.Fence.Topic || t.metadata.Fence.Partition != u.metadata.Fence.Partition || incoming.StartOffset != old.EndOffset || incoming.EndOffset <= old.EndOffset {
		return result, fmt.Errorf("detached suffix identity, adjacency, or expected extent mismatch")
	}
	raw, index, err := openDetachedAppendFiles(t)
	if err != nil {
		return result, err
	}
	defer func() { _ = raw.Close(); _ = index.Close() }()
	if err = repairDetachedTail(t, raw, index); err != nil {
		return result, fmt.Errorf("detached append tail cleanup: %w", err)
	}
	sourceRaw, err := os.Open(filepath.Join(u.dir, "records"))
	if err != nil {
		return result, err
	}
	defer func() { _ = sourceRaw.Close() }()
	sourceIndex, err := os.Open(filepath.Join(u.dir, "range-index"))
	if err != nil {
		return result, err
	}
	defer func() { _ = sourceIndex.Close() }()
	// #nosec G115 -- published extents contain validated nonnegative file sizes; lastIndex is within the old raw extent.
	boundary := incoming.RawBytes != 0 && uint64(old.RawBytes)-c.lastIndex >= detachedIndexInterval
	indexGrowth := incoming.IndexBytes
	if boundary {
		indexGrowth += 16
	}
	newRaw, err := addBootstrapBytes(old.RawBytes, incoming.RawBytes)
	if err != nil {
		return result, err
	}
	newIndex, err := addBootstrapBytes(old.IndexBytes, indexGrowth)
	if err != nil {
		return result, err
	}
	growth, err := addBootstrapBytes(incoming.RawBytes, indexGrowth)
	if err != nil {
		return result, err
	}
	reservation, err := addBootstrapBytes(t.byteOwner.reserved, growth)
	if err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	previousReservation := t.byteOwner.reserved
	if err = t.byteOwner.growDetached(c.root.headroom, reservation); err != nil {
		return result, err
	}
	// Reservation includes both complete source files and destination growth.
	// The initial index estimate's slack stays fixed rather than accumulating.
	c.tailDebt = &detachedTailDebt{bounds: old, reservation: previousReservation}
	source.retired = true
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, repairDetachedTail(t, raw, index))
		}
		// Close source handles before removal, including on Windows.
		_ = sourceRaw.Close()
		_ = sourceIndex.Close()
		result.CleanupError = u.cleanupLocked()
	}()
	if err = copyDetachedSuffix(ctx, c.root.appendIO, raw, sourceRaw, old.RawBytes, incoming.RawBytes, &result.Work); err != nil {
		return result, err
	}
	lastIndex, err := appendDetachedIndex(ctx, c.root.appendIO, index, sourceIndex, old, source, boundary, c.lastIndex, &result.Work)
	if err != nil {
		return result, err
	}
	if err = errors.Join(c.root.appendIO.sync(raw), c.root.appendIO.sync(index)); err != nil {
		return result, err
	}
	if c.root.appendIO.beforePublish != nil {
		if err = c.root.appendIO.beforePublish(); err != nil {
			return result, err
		}
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	c.root.mu.Lock()
	defer c.root.mu.Unlock()
	if c.root.closing {
		return result, ErrDetachedBootstrapClosed
	}
	c.extent = DetachedRawBounds{StartOffset: old.StartOffset, EndOffset: incoming.EndOffset, RawBytes: newRaw, IndexBytes: newIndex}
	c.lastIndex = lastIndex
	c.tailDebt = nil
	result.Bounds = c.extent
	return result, nil
}

func copyDetachedSuffix(ctx context.Context, ops detachedAppendIO, target, source *os.File, offset, size int64, work *DetachedAppendWork) error {
	if size == 0 {
		return ctx.Err()
	}
	buffer := make([]byte, BootstrapChunkBytes)
	for position := int64(0); position < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := buffer[:min(int64(len(buffer)), size-position)]
		n, err := source.ReadAt(chunk, position)
		// #nosec G115 -- os.File.ReadAt returns a nonnegative byte count, including partial failures.
		work.RawReadBytes += uint64(n)
		if err != nil {
			return err
		}
		n, err = ops.writeAt(target, chunk, offset+position)
		// #nosec G115 -- the private I/O adapter follows os.File.WriteAt's nonnegative byte-count contract.
		work.RawBytes += uint64(n)
		if err != nil {
			return err
		}
		if n != len(chunk) {
			return io.ErrShortWrite
		}
		position += int64(n)
	}
	return nil
}

func appendDetachedIndex(ctx context.Context, ops detachedAppendIO, target, source *os.File, old DetachedRawBounds, suffix *detachedBootstrapChild, boundary bool, lastIndex uint64, work *DetachedAppendWork) (uint64, error) {
	position := old.IndexBytes
	write := func(entry []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := ops.writeAt(target, entry, position)
		// #nosec G115 -- the private I/O adapter follows os.File.WriteAt's nonnegative byte-count contract.
		work.IndexBytes += uint64(n)
		if err != nil {
			return err
		}
		if n != len(entry) {
			return io.ErrShortWrite
		}
		position += int64(n)
		lastIndex = binary.BigEndian.Uint64(entry[8:])
		return nil
	}
	var entry [16]byte
	if boundary {
		binary.BigEndian.PutUint64(entry[:8], suffix.firstOffset)
		// #nosec G115 -- old.RawBytes is a published, validated nonnegative extent size.
		binary.BigEndian.PutUint64(entry[8:], uint64(old.RawBytes))
		if err := write(entry[:]); err != nil {
			return lastIndex, err
		}
	}
	for pos := int64(0); pos < suffix.extent.IndexBytes; pos += 16 {
		if err := ctx.Err(); err != nil {
			return lastIndex, err
		}
		n, err := source.ReadAt(entry[:], pos)
		// #nosec G115 -- os.File.ReadAt returns a nonnegative byte count bounded by the entry buffer.
		work.IndexReadBytes += uint64(n)
		if err != nil {
			return lastIndex, err
		}
		// #nosec G115 -- old.RawBytes is nonnegative; the combined raw extent was checked by addBootstrapBytes before append.
		physical := binary.BigEndian.Uint64(entry[8:]) + uint64(old.RawBytes)
		binary.BigEndian.PutUint64(entry[8:], physical)
		if err := write(entry[:]); err != nil {
			return lastIndex, err
		}
	}
	return lastIndex, nil
}
