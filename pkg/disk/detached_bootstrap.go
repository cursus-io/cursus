package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrDetachedBootstrapPending = errors.New("detached bootstrap close awaits active work, leases, or cleanup")
var ErrDetachedBootstrapClosed = errors.New("detached bootstrap owner is closed")

const detachedBootstrapPrefix = ".detached.bootstrap-"
const detachedIndexInterval uint64 = 4096

// DetachedBootstrapRoot owns ephemeral recovery files and their exclusive lock.
// It has no partition handler, durable intent, or background cleanup worker.
type DetachedBootstrapRoot struct {
	mu           sync.Mutex
	closeMu      sync.Mutex
	dir          string
	limits       BootstrapLimits
	headroom     *diskHeadroomGuard
	lock         *StorageLock
	closing      bool
	finalized    bool
	children     map[*bootstrapTransfer]struct{}
	maxAdopted   int
	adopted      map[*bootstrapTransfer]struct{}
	adoptedOrder []*bootstrapTransfer
	// Instance seams run outside mu; configure before concurrent use.
	beforeCreate         func()
	beforePreparePublish func()
	openFile             func(string) (*os.File, error)
	appendIO             detachedAppendIO
}

type detachedBootstrapChild struct {
	root        *DetachedBootstrapRoot
	prepared    bool
	retired     bool
	leases      int
	adopted     bool
	extent      DetachedRawBounds
	firstOffset uint64
	lastIndex   uint64
	tailDebt    *detachedTailDebt
}

type DetachedBootstrapStage struct{ transfer *bootstrapTransfer }
type DetachedRawStore struct{ transfer *bootstrapTransfer }

// NewDetachedBootstrapRoot removes only recognized ephemeral child directories.
// Unknown names, lock files, and durable partition bootstrap names are preserved.
// maxAdoptedStores bounds all cached generations, including retired, pinned, and
// failed-cleanup stores. Callers derive it from their bounded partition vector
// plus retired-generation allowance, independently of active-transfer limits.
func NewDetachedBootstrapRoot(dir string, limits BootstrapLimits, minFreeBytes int64, minFreePercent float64, maxAdoptedStores int) (*DetachedBootstrapRoot, error) {
	return newDetachedBootstrapRoot(dir, limits, minFreeBytes, minFreePercent, maxAdoptedStores, os.RemoveAll)
}

func newDetachedBootstrapRoot(dir string, limits BootstrapLimits, minFreeBytes int64, minFreePercent float64, maxAdoptedStores int, removeOrphan func(string) error) (*DetachedBootstrapRoot, error) {
	if maxAdoptedStores <= 0 {
		return nil, fmt.Errorf("detached bootstrap requires positive adopted-store limit")
	}
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("detached bootstrap directory is empty")
	}
	if err := validateBootstrapLimits(limits); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid detached bootstrap root")
	}
	lockPath := filepath.Join(path, ".cursus.lock")
	if info, err = os.Lstat(lockPath); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("invalid detached bootstrap lock")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	lock, err := LockStorageDirectory(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(path)
	if err == nil {
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), detachedBootstrapPrefix) || !validBootstrapID(strings.TrimPrefix(entry.Name(), detachedBootstrapPrefix)) {
				continue
			}
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				err = fmt.Errorf("non-directory detached bootstrap child")
				break
			}
			if err = removeOrphan(filepath.Join(path, entry.Name())); err != nil {
				break
			}
		}
	}
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &DetachedBootstrapRoot{dir: path, limits: limits, headroom: newDiskHeadroomGuard(path, minFreeBytes, minFreePercent), lock: lock, children: make(map[*bootstrapTransfer]struct{}), openFile: os.Open, maxAdopted: maxAdoptedStores, adopted: make(map[*bootstrapTransfer]struct{}), appendIO: defaultDetachedAppendIO()}, nil
}

func (r *DetachedBootstrapRoot) forget(t *bootstrapTransfer) {
	r.mu.Lock()
	delete(r.children, t)
	delete(r.adopted, t)
	for i, child := range r.adoptedOrder {
		if child == t {
			copy(r.adoptedOrder[i:], r.adoptedOrder[i+1:])
			r.adoptedOrder[len(r.adoptedOrder)-1] = nil
			r.adoptedOrder = r.adoptedOrder[:len(r.adoptedOrder)-1]
			break
		}
	}
	r.mu.Unlock()
}

func (r *DetachedBootstrapRoot) isClosing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closing
}

// Close retires new work immediately. Retry explicitly after busy operations or
// leases finish; the kernel lock survives until every physical child is gone.
func (r *DetachedBootstrapRoot) Close() error {
	r.mu.Lock()
	r.closing = true
	children := make([]*bootstrapTransfer, 0, len(r.children))
	for t := range r.children {
		children = append(children, t)
	}
	r.mu.Unlock()
	if !r.closeMu.TryLock() {
		return ErrDetachedBootstrapPending
	}
	defer r.closeMu.Unlock()
	var cleanupErr error
	for _, t := range children {
		if !t.mu.TryLock() {
			continue
		}
		t.detached.retired = true
		cleanupErr = errors.Join(cleanupErr, t.cleanupLocked())
		t.mu.Unlock()
	}
	r.mu.Lock()
	pending, finalized := len(r.children) != 0, r.finalized
	r.mu.Unlock()
	if pending {
		return errors.Join(ErrDetachedBootstrapPending, cleanupErr)
	}
	if finalized {
		return nil
	}
	if err := r.lock.Close(); err != nil {
		return err
	}
	r.mu.Lock()
	r.finalized = true
	r.mu.Unlock()
	return nil
}

// Stage reserves all incoming and derived bytes before creating a private child.
// This metadata-only format requires the empty snapshot-catalog descriptor.
func (r *DetachedBootstrapRoot) Stage(topic string, partition int, metadata BootstrapMetadata) (*DetachedBootstrapStage, error) {
	r.retryAdoptedCleanup()
	size, err := validateBootstrapMetadata(metadata)
	if err != nil {
		return nil, err
	}
	if metadata.Fence.Topic != topic || metadata.Fence.Partition != partition {
		return nil, fmt.Errorf("detached bootstrap identity mismatch")
	}
	empty := sha256.Sum256(nil)
	if metadata.Files[1].Size != 0 || metadata.Files[1].SHA256 != hex.EncodeToString(empty[:]) {
		return nil, fmt.Errorf("detached bootstrap requires empty catalog")
	}
	if metadata.Files[0].Size > 0 {
		size, err = addBootstrapBytes(size, (metadata.Files[0].Size/int64(detachedIndexInterval)+1)*16)
		if err != nil {
			return nil, err
		}
	}
	t := &bootstrapTransfer{detached: &detachedBootstrapChild{root: r}}
	t.mu.Lock()
	defer t.mu.Unlock()
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil, ErrDetachedBootstrapClosed
	}
	r.children[t] = struct{}{}
	r.mu.Unlock()
	// From this point even creation in progress is represented in root ownership.
	if r.beforeCreate != nil {
		r.beforeCreate()
	}
	if r.isClosing() {
		r.forget(t)
		return nil, ErrDetachedBootstrapClosed
	}
	retryBootstrapCleanup()
	// #nosec G115 -- validateBootstrapMetadata and checked derived-index addition produced a nonnegative int64 size.
	if _, err = r.headroom.reserve(uint64(size), true); err != nil {
		r.forget(t)
		return nil, err
	}
	if err = initializeBootstrapTransfer(t, filepath.Join(r.dir, ".detached"), metadata, size, r.limits); err != nil {
		r.forget(t)
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = t.cleanupLocked()
		}
	}()
	for _, f := range metadata.Files {
		if err = writeBootstrapFile(filepath.Join(t.dir, f.ID), nil); err != nil {
			return nil, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, ErrDetachedBootstrapClosed
	}
	t.startTTL()
	ok = true
	return &DetachedBootstrapStage{transfer: t}, nil
}

func (s *DetachedBootstrapStage) Manifest() BootstrapMetadata {
	return cloneBootstrapMetadata(s.transfer.metadata)
}
func (s *DetachedBootstrapStage) Abort() error { return s.transfer.close() }

func (s *DetachedBootstrapStage) WriteChunk(fileID string, offset int64, data []byte) error {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.detached.prepared || t.detached.root.isClosing() {
		return ErrDetachedBootstrapClosed
	}
	f, err := bootstrapFile(t.metadata, fileID)
	if err != nil {
		return err
	}
	if offset < 0 || len(data) > BootstrapChunkBytes || int64(len(data)) > f.Size || offset > f.Size-int64(len(data)) {
		return fmt.Errorf("invalid bootstrap chunk bounds")
	}
	return writeBootstrapChunkLocked(t, fileID, offset, data)
}

// Prepare scans and verifies once. The same physical reservation and child
// identity move to the store; stale stage and TTL handles lose cleanup authority.
func (s *DetachedBootstrapStage) Prepare(ctx context.Context) (*DetachedRawStore, error) {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.detached.prepared || t.detached.root.isClosing() {
		return nil, ErrDetachedBootstrapClosed
	}
	ok := false
	defer func() {
		if !ok {
			_ = t.cleanupLocked()
		}
	}()
	if ctx == nil {
		return nil, fmt.Errorf("detached preparation requires context")
	}
	if err := verifyBootstrapIncoming(ctx, t); err != nil {
		return nil, err
	}
	if err := buildBootstrapCanonicalIndex(ctx, t, detachedIndexInterval, 0); err != nil {
		return nil, err
	}
	if err := syncBootstrapDirectory(t.dir); err != nil {
		return nil, err
	}
	info, err := os.Stat(filepath.Join(t.dir, "range-index"))
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	r := t.detached.root
	if r.beforePreparePublish != nil {
		r.beforePreparePublish()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, ErrDetachedBootstrapClosed
	}
	t.detached.extent = DetachedRawBounds{StartOffset: t.metadata.StartOffset, EndOffset: t.metadata.EndOffset, RawBytes: t.metadata.Files[0].Size, IndexBytes: info.Size()}
	t.detached.prepared = true
	t.timer.Stop()
	ok = true
	return &DetachedRawStore{transfer: t}, nil
}

// Close prevents new leases while existing leases keep their files and bounds.
func (s *DetachedRawStore) Close() error {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached.adopted {
		return nil
	}
	t.detached.retired = true
	return t.cleanupLocked()
}

func (s *DetachedRawStore) Acquire() (*DetachedRawLease, error) {
	t := s.transfer
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached.adopted {
		return nil, ErrDetachedBootstrapClosed
	}
	return acquireDetachedLocked(t)
}

func acquireDetachedLocked(t *bootstrapTransfer) (*DetachedRawLease, error) {
	child := t.detached
	if child.retired || t.closed || child.root.isClosing() {
		return nil, ErrDetachedBootstrapClosed
	}
	records, err := child.root.openFile(filepath.Join(t.dir, "records"))
	if err != nil {
		return nil, err
	}
	index, err := child.root.openFile(filepath.Join(t.dir, "range-index"))
	if err != nil {
		_ = records.Close()
		return nil, err
	}
	child.root.mu.Lock()
	if child.root.closing {
		child.root.mu.Unlock()
		_ = records.Close()
		_ = index.Close()
		return nil, ErrDetachedBootstrapClosed
	}
	child.leases++
	child.root.mu.Unlock()
	b := child.extent
	return &DetachedRawLease{transfer: t, records: records, index: index, start: b.StartOffset, end: b.EndOffset, rawBytes: b.RawBytes, indexBytes: b.IndexBytes, next: b.StartOffset}, nil
}
