package disk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// RetainedBootstrapWork counts this export's page attempts (including oversized
// retries), actual canonical writes including frame headers, and checksum reads.
// Source read/decoding work is separately available from RetainedPageWorkSnapshot;
// deltas of that handler-wide counter may include concurrent requests.
type RetainedBootstrapWork struct {
	Pages, CanonicalWriteBytes, HashReadBytes uint64
}

// RetainedWork returns immutable diagnostics for a successful retained export.
// The generic catalog export does not collect these counters.
func (e *BootstrapExport) RetainedWork() RetainedBootstrapWork { return e.retainedWork }

// RetainedBootstrapWorkSnapshot reports cumulative completed export attempts,
// including failed and canceled work. Concurrent exports contribute to deltas;
// in-flight attempts are added when they return. This is diagnostic, not a fence.
func (d *DiskHandler) RetainedBootstrapWorkSnapshot() RetainedBootstrapWork {
	d.retainedBootstrapWorkMu.Lock()
	defer d.retainedBootstrapWorkMu.Unlock()
	return d.retainedBootstrapWork
}

func (d *DiskHandler) recordRetainedBootstrapWork(work RetainedBootstrapWork) {
	d.retainedBootstrapWorkMu.Lock()
	defer d.retainedBootstrapWorkMu.Unlock()
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoveryPages, work.Pages)
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoveryWriteBytes, work.CanonicalWriteBytes)
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoveryHashBytes, work.HashReadBytes)
	d.retainedBootstrapWork.Pages += work.Pages
	d.retainedBootstrapWork.CanonicalWriteBytes += work.CanonicalWriteBytes
	d.retainedBootstrapWork.HashReadBytes += work.HashReadBytes
}

// Per-call operations keep failure tests independent of live-handler I/O hooks.
// These operations run only on private export files, outside storage locks.
type retainedExportIO struct {
	serialize     func(types.DiskMessage) ([]byte, error)
	write         func(*os.File, []byte) (int, error)
	read          func(*os.File, []byte) (int, error)
	sync          func(*os.File) error
	syncDirectory func(string) error
	removeAll     func(string) error
}

func defaultRetainedExportIO() retainedExportIO {
	return retainedExportIO{
		serialize: util.SerializeDiskMessage,
		write:     (*os.File).Write, read: (*os.File).Read, sync: (*os.File).Sync,
		syncDirectory: syncBootstrapDirectory, removeAll: os.RemoveAll,
	}
}

// ExportRetainedBootstrapContext prepares a metadata-only canonical export with
// an empty catalog. Capture expected BEFORE reading the retained anchor or fixed
// range. expectedAnchor is the retained start, independent of a suffix's start.
// Appends and normal rolls may proceed; destructive mutations invalidate expected.
//
// The caller must establish a bounded creating owner with a finite lifetime and
// cancellation before calling, and must hold no partition/storage locks. There
// is no automatic TTL: on success the caller owns Close, including cleanup retry
// responsibility. A later registry must separately fence publication with the
// ORIGINAL token and establish its ready TTL. Returning a private prepared handle
// does not authorize cluster recovery or publish it in a registry.
//
// Pages bound memory and storage-lock occupancy; serialization, writes, checksums
// and export syncs hold no storage locks. Cancellation is checked between records
// and checksum chunks. Individual filesystem calls, including fsync, cannot be
// preempted. Cold seeks or checkpoint eviction may repeat source-prefix work.
func (d *DiskHandler) ExportRetainedBootstrapContext(ctx context.Context, expected ContentGeneration, expectedAnchor, start, end uint64, fence BootstrapFence) (*BootstrapExport, error) {
	return d.exportRetainedBootstrap(ctx, expected, expectedAnchor, start, end, fence, defaultRetainedExportIO())
}

func (d *DiskHandler) checkRetainedExportSource(ctx context.Context, expected ContentGeneration, anchor uint64) error {
	unlock, err := lockProofContext(ctx, &d.mu, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.proofAvailabilityLocked(); err != nil {
		return err
	}
	if expected != d.contentGenerationLocked() || len(d.segments) == 0 || d.segments[0] != anchor {
		return ErrStaleContentGeneration
	}
	return nil
}

func (d *DiskHandler) exportRetainedBootstrap(ctx context.Context, expected ContentGeneration, anchor, start, end uint64, fence BootstrapFence, ops retainedExportIO) (_ *BootstrapExport, resultErr error) {
	var work RetainedBootstrapWork
	defer func() {
		d.recordRetainedBootstrapWork(work)
		metrics.RecordRecoveryAttempt(metrics.RecoveryRetainedExport, resultErr)
	}()
	if ctx == nil {
		return nil, fmt.Errorf("retained bootstrap requires context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, err := newBootstrapID()
	if err != nil {
		return nil, err
	}
	m := BootstrapMetadata{Version: BootstrapVersion, TransferID: id, StartOffset: start, EndOffset: end, Fence: fence}
	if err = validateBootstrapBounds(m); err != nil {
		return nil, err
	}
	if start < anchor {
		return nil, fmt.Errorf("retained bootstrap starts before retained anchor")
	}
	if filepath.Base(d.BaseName) != fmt.Sprintf("partition_%d", fence.Partition) || filepath.Base(filepath.Dir(d.BaseName)) != fence.Topic {
		return nil, fmt.Errorf("bootstrap source identity mismatch")
	}
	if err = d.checkRetainedExportSource(ctx, expected, anchor); err != nil {
		return nil, err
	}
	retryBootstrapCleanup()
	if _, err = d.headroom.reserve(0, true); err != nil {
		return nil, err
	}
	t, err := newBootstrapTransferAfterCleanup(d.BaseName, m, 0, d.BootstrapLimits())
	if err != nil {
		return nil, err
	}
	t.removeAll = ops.removeAll
	success := false
	defer func() {
		if !success {
			_ = t.close()
		}
	}()
	log, err := os.OpenFile(filepath.Join(t.dir, "records"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = log.Close() }()
	var outputBytes int64
	position := start
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		work.Pages++
		page, pageErr := d.ReadRetainedPage(ctx, position, end, 1024, 1<<20)
		if errors.Is(pageErr, ErrRetainedPageLimit) {
			work.Pages++
			page, pageErr = d.ReadRetainedPage(ctx, position, end, 1, MaxMessageSize)
		}
		if pageErr != nil {
			return nil, pageErr
		}
		if page.ContentGeneration != expected {
			return nil, ErrStaleContentGeneration
		}
		if page.EndOffset < position || page.EndOffset > end || (!page.Exhausted && page.EndOffset == position) || (page.Exhausted && page.EndOffset != end) {
			return nil, fmt.Errorf("retained bootstrap page made invalid progress")
		}
		m.Compacted = m.Compacted || page.Compacted
		for _, message := range page.Messages {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			record := retainedDiskMessage(message)
			if record.Topic != fence.Topic || int(record.Partition) != fence.Partition {
				return nil, fmt.Errorf("bootstrap record identity mismatch")
			}
			canonical, serializeErr := ops.serialize(record)
			if serializeErr != nil {
				return nil, serializeErr
			}
			if err = validateSerializedDiskMessageSize(canonical); err != nil {
				return nil, err
			}
			length, ok := util.SafeIntToUint32(len(canonical))
			if !ok {
				return nil, fmt.Errorf("bootstrap canonical frame too large")
			}
			next, addErr := addBootstrapBytes(outputBytes, 4+int64(len(canonical)))
			if addErr != nil {
				return nil, addErr
			}
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			if err = t.byteOwner.growDetached(d.headroom, next); err != nil {
				return nil, err
			}
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], length)
			for _, data := range [][]byte{header[:], canonical} {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
				n, writeErr := ops.write(log, data)
				// #nosec G115 -- the private I/O adapter follows os.File.Write's nonnegative byte-count contract.
				work.CanonicalWriteBytes += uint64(n)
				if writeErr != nil {
					return nil, writeErr
				}
				if n != len(data) {
					return nil, io.ErrShortWrite
				}
			}
			outputBytes = next
		}
		if page.Exhausted {
			break
		}
		position = page.EndOffset
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoverySyncCalls, 1)
	if err = ops.sync(log); err != nil {
		return nil, err
	}
	if err = log.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	catalog, err := os.OpenFile(filepath.Join(t.dir, "catalog"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoverySyncCalls, 1)
	syncErr := ops.sync(catalog)
	closeErr := catalog.Close()
	if err = errors.Join(syncErr, closeErr); err != nil {
		return nil, err
	}
	for _, entry := range []struct{ id, role string }{{"records", "canonical-log"}, {"catalog", "snapshot-catalog"}} {
		descriptor, describeErr := describeRetainedExportFile(ctx, filepath.Join(t.dir, entry.id), entry.id, entry.role, ops, &work)
		if describeErr != nil {
			return nil, describeErr
		}
		m.Files = append(m.Files, descriptor)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedExport, metrics.RecoverySyncCalls, 1)
	if err = ops.syncDirectory(t.dir); err != nil {
		return nil, err
	}
	if err = d.checkRetainedExportSource(ctx, expected, anchor); err != nil {
		return nil, err
	}
	t.metadata = m
	success = true
	return &BootstrapExport{transfer: t, retainedWork: work}, nil
}

func describeRetainedExportFile(ctx context.Context, path, id, role string, ops retainedExportIO, work *RetainedBootstrapWork) (BootstrapFile, error) {
	if err := ctx.Err(); err != nil {
		return BootstrapFile{}, err
	}
	// #nosec G304 -- the only caller builds path from its private staging directory and fixed records/catalog names.
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
	buffer := make([]byte, BootstrapChunkBytes)
	for {
		if err = ctx.Err(); err != nil {
			return BootstrapFile{}, err
		}
		n, readErr := ops.read(f, buffer)
		// #nosec G115 -- the private I/O adapter follows os.File.Read's nonnegative byte-count contract.
		work.HashReadBytes += uint64(n)
		if n > 0 {
			_, _ = h.Write(buffer[:n])
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return BootstrapFile{}, readErr
			}
			break
		}
		if n == 0 {
			return BootstrapFile{}, io.ErrNoProgress
		}
	}
	if err = ctx.Err(); err != nil {
		return BootstrapFile{}, err
	}
	return BootstrapFile{ID: id, Role: role, Size: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// This is the lossless inverse of retainedMessage, without wire catch-up
// normalization of transaction, control, identity, or event metadata.
func retainedDiskMessage(m types.Message) types.DiskMessage {
	return types.DiskMessage{
		// #nosec G115 -- retainedMessage widened this validated on-disk int32 partition; this restores its original type.
		Topic: m.Topic, Partition: int32(m.Partition), Offset: m.Offset, Payload: m.Payload,
		ProducerID: m.ProducerID, SeqNum: m.SeqNum, Epoch: m.Epoch, LeaderEpoch: m.LeaderEpoch,
		Key: m.Key, EventType: m.EventType, SchemaVersion: m.SchemaVersion, AggregateVersion: m.AggregateVersion,
		Metadata: m.Metadata, EventID: m.EventID, PayloadDigest: m.PayloadDigest,
		TransactionalID: m.TransactionalID, TransactionState: m.TransactionState, TransactionMarker: m.TransactionMarker,
		ControlBatchType: m.ControlBatchType, ControlBatchVersion: m.ControlBatchVersion,
		ControlBatchCoordinatorEpoch: m.ControlBatchCoordinatorEpoch, ControlBatchKey: m.ControlBatchKey, ControlBatchValue: m.ControlBatchValue,
	}
}
