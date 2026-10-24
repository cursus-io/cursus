package topic

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const detachedMetadataPageBytes = 1 << 20
const detachedMetadataPageRecords = 1024

// ErrDetachedMetadataLimit is retryable after other owners release resources.
var ErrDetachedMetadataLimit = errors.New("detached metadata budget exhausted")

type detachedMetadataLimits struct{ keys, slots, ids, bytes int64 }
type detachedMetadataCharge struct{ keys, slots, ids, bytes int64 }

// DetachedMetadataBudget bounds history across all stores sharing this budget.
// Charges conservatively model maps (512 bytes/key), backing capacities (64
// bytes/slot), and retained ID bytes, rather than promising exact Go heap size.
// One permit reserves 256 MiB for decode scratch, independently of history.
// Returned pages are caller-owned and bounded separately.
type DetachedMetadataBudget struct {
	mu      sync.Mutex
	limits  detachedMetadataLimits
	used    detachedMetadataCharge
	scratch chan struct{}
	work    disk.DetachedRawWork
}

func NewDetachedMetadataBudget() *DetachedMetadataBudget {
	return newDetachedMetadataBudget(detachedMetadataLimits{65536, 1048576, 8 << 20, 128 << 20})
}
func newDetachedMetadataBudget(limits detachedMetadataLimits) *DetachedMetadataBudget {
	return &DetachedMetadataBudget{limits: limits, scratch: make(chan struct{}, 1)}
}

// Work includes actual raw work across this budget's owners, including failed
// constructors, suffix scans, failed reads, and retries. It survives retirement.
func (b *DetachedMetadataBudget) Work() disk.DetachedRawWork {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.work
}
func (b *DetachedMetadataBudget) addWork(work disk.DetachedRawWork) {
	b.mu.Lock()
	defer b.mu.Unlock()
	addDetachedRawWork(&b.work, work)
}
func (b *DetachedMetadataBudget) reserve(c detachedMetadataCharge) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	u, lim := b.used, b.limits
	if c.keys > lim.keys-u.keys || c.slots > lim.slots-u.slots || c.ids > lim.ids-u.ids || c.bytes > lim.bytes-u.bytes {
		return ErrDetachedMetadataLimit
	}
	b.used.keys += c.keys
	b.used.slots += c.slots
	b.used.ids += c.ids
	b.used.bytes += c.bytes
	return nil
}
func (b *DetachedMetadataBudget) release(c detachedMetadataCharge) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used.keys -= c.keys
	b.used.slots -= c.slots
	b.used.ids -= c.ids
	b.used.bytes -= c.bytes
}
func (b *DetachedMetadataBudget) acquireScratch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case b.scratch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type detachedMarkerEntries struct {
	entries []transactionMarkerInfo
}

type detachedMarkerHistory struct {
	entries map[transactionMarkerKey]*detachedMarkerEntries
	charge  detachedMetadataCharge
	budget  *DetachedMetadataBudget
}

func newDetachedMarkerHistory(b *DetachedMetadataBudget) (*detachedMarkerHistory, error) {
	c := detachedMetadataCharge{bytes: 512}
	if err := b.reserve(c); err != nil {
		return nil, err
	}
	return &detachedMarkerHistory{entries: make(map[transactionMarkerKey]*detachedMarkerEntries), budget: b, charge: c}, nil
}
func (h *detachedMarkerHistory) release() {
	h.entries = nil
	h.budget.release(h.charge)
	h.charge = detachedMetadataCharge{}
}

// ensure reserves peak old+replacement capacities before allocating. Expanded
// capacity and empty keys may remain charged after a failed append.
func (h *detachedMarkerHistory) ensure(key transactionMarkerKey, extra int) (*detachedMarkerEntries, error) {
	list, exists := h.entries[key]
	var entries []transactionMarkerInfo
	if exists {
		entries = list.entries
	}
	if !exists {
		c := detachedMetadataCharge{keys: 1, ids: int64(len(key.transactionalID)), bytes: 512 + int64(len(key.transactionalID))}
		if err := h.budget.reserve(c); err != nil {
			return nil, err
		}
		h.charge.keys += c.keys
		h.charge.ids += c.ids
		h.charge.bytes += c.bytes
		key.transactionalID = strings.Clone(key.transactionalID)
		list = &detachedMarkerEntries{}
		h.entries[key] = list
	}
	if len(entries)+extra <= cap(entries) {
		return list, nil
	}
	capacity := max(1, cap(entries))
	for capacity < len(entries)+extra {
		capacity *= 2
	}
	c := detachedMetadataCharge{slots: int64(capacity), bytes: int64(capacity) * 64}
	if err := h.budget.reserve(c); err != nil {
		return nil, err
	}
	replacement := make([]transactionMarkerInfo, len(entries), capacity)
	copy(replacement, entries)
	list.entries = replacement
	old := detachedMetadataCharge{slots: int64(cap(entries)), bytes: int64(cap(entries)) * 64}
	h.budget.release(old)
	h.charge.slots += c.slots - old.slots
	h.charge.bytes += c.bytes - old.bytes
	return list, nil
}

// DetachedMetadataWork reports cumulative actual raw scan work, including failed
// attempts, oversized retries, sparse-index seeks, and discarded decoded records.
type DetachedMetadataWork struct {
	Initial, Suffix, Reads disk.DetachedRawWork
	Append                 disk.DetachedAppendWork
}

// DetachedMetadataStore owns an adopted raw generation after construction
// succeeds. Retirement prevents admission; existing leases retain their history
// until the last lease closes, independently of physical cleanup success.
type DetachedMetadataStore struct {
	mu             sync.Mutex
	historyMu      sync.RWMutex
	workMu         sync.Mutex
	raw            *disk.AdoptedRawStore
	budget         *DetachedMetadataBudget
	history        *detachedMarkerHistory
	retired        bool
	leases         int
	work           DetachedMetadataWork
	appendPrepared func(context.Context, disk.DetachedRawBounds, *disk.DetachedRawStore) (disk.DetachedAppendResult, error)
	closeRaw       func() error
}

func NewDetachedMetadataStore(ctx context.Context, raw *disk.AdoptedRawStore, budget *DetachedMetadataBudget) (*DetachedMetadataStore, error) {
	if ctx == nil || raw == nil || budget == nil {
		return nil, fmt.Errorf("detached metadata requires context, raw store, and shared budget")
	}
	history, err := newDetachedMarkerHistory(budget)
	if err != nil {
		return nil, err
	}
	lease, err := raw.Acquire()
	if err != nil {
		history.release()
		return nil, err
	}
	err = scanDetachedMarkers(ctx, lease, history)
	work := lease.Work()
	budget.addWork(work)
	err = errors.Join(err, lease.Close())
	if err != nil {
		history.release()
		return nil, err
	}
	return &DetachedMetadataStore{raw: raw, budget: budget, history: history, work: DetachedMetadataWork{Initial: work}, appendPrepared: raw.AppendPrepared, closeRaw: raw.Close}, nil
}

func readDetachedOne(ctx context.Context, lease *disk.DetachedRawLease, offset uint64) (disk.DetachedRawPage, error) {
	page, err := lease.ReadRaw(ctx, offset, 1, detachedMetadataPageBytes)
	if errors.Is(err, disk.ErrRetainedPageLimit) {
		return lease.ReadRaw(ctx, offset, 1, disk.MaxMessageSize)
	}
	return page, err
}
func scanDetachedMarkers(ctx context.Context, lease *disk.DetachedRawLease, h *detachedMarkerHistory) error {
	if err := h.budget.acquireScratch(ctx); err != nil {
		return err
	}
	defer func() { <-h.budget.scratch }()
	for offset := lease.Bounds().StartOffset; offset < lease.Bounds().EndOffset; {
		page, err := readDetachedOne(ctx, lease, offset)
		if err != nil {
			return err
		}
		for _, m := range page.Messages {
			if m.TransactionalID == "" || m.TransactionMarker == types.TransactionMarkerNone {
				continue
			}
			list, err := h.ensure(transactionMarkerKey{m.TransactionalID, m.Epoch}, 1)
			if err != nil {
				return err
			}
			// Retain canonical constants, never an uncharged decoded marker string.
			marker := "unknown"
			switch m.TransactionMarker {
			case types.TransactionMarkerCommit:
				marker = types.TransactionMarkerCommit
			case types.TransactionMarkerAbort:
				marker = types.TransactionMarkerAbort
			}
			list.entries = append(list.entries, transactionMarkerInfo{marker: marker, offset: m.Offset, coordinatorEpoch: m.ControlBatchCoordinatorEpoch})
		}
		if page.Exhausted {
			return nil
		}
		if page.NextOffset <= offset {
			return fmt.Errorf("detached marker scan made no progress")
		}
		offset = page.NextOffset
	}
	return ctx.Err()
}

func (s *DetachedMetadataStore) Acquire() (*DetachedMetadataLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, disk.ErrDetachedBootstrapClosed
	}
	raw, err := s.raw.Acquire()
	if err != nil {
		return nil, err
	}
	s.leases++
	return &DetachedMetadataLease{store: s, raw: raw, closeRaw: raw.Close}, nil
}
func (s *DetachedMetadataStore) Work() DetachedMetadataWork {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	return s.work
}
func addDetachedRawWork(target *disk.DetachedRawWork, value disk.DetachedRawWork) {
	target.ReadBytes += value.ReadBytes
	target.IndexReadBytes += value.IndexReadBytes
	target.DecodedRecords += value.DecodedRecords
}
func (s *DetachedMetadataStore) AppendPrepared(ctx context.Context, expected disk.DetachedRawBounds, suffix *disk.DetachedRawStore) (disk.DetachedAppendResult, error) {
	if ctx == nil || suffix == nil {
		return disk.DetachedAppendResult{}, fmt.Errorf("detached append requires context and suffix")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return disk.DetachedAppendResult{}, disk.ErrDetachedBootstrapClosed
	}
	delta, err := newDetachedMarkerHistory(s.budget)
	if err != nil {
		return disk.DetachedAppendResult{}, err
	}
	defer delta.release()
	scan, err := suffix.Acquire()
	if err != nil {
		return disk.DetachedAppendResult{}, err
	}
	err = scanDetachedMarkers(ctx, scan, delta)
	s.workMu.Lock()
	addDetachedRawWork(&s.work.Suffix, scan.Work())
	s.budget.addWork(scan.Work())
	s.workMu.Unlock()
	err = errors.Join(err, scan.Close())
	if err != nil {
		return disk.DetachedAppendResult{}, err
	}
	s.historyMu.Lock()
	for key, entries := range delta.entries {
		if _, err = s.history.ensure(key, len(entries.entries)); err != nil {
			break
		}
	}
	s.historyMu.Unlock()
	if err != nil {
		return disk.DetachedAppendResult{}, err
	}
	result, err := s.appendPrepared(ctx, expected, suffix)
	s.workMu.Lock()
	s.work.Append.RawBytes += result.Work.RawBytes
	s.work.Append.IndexBytes += result.Work.IndexBytes
	s.work.Append.RawReadBytes += result.Work.RawReadBytes
	s.work.Append.IndexReadBytes += result.Work.IndexReadBytes
	s.workMu.Unlock()
	if err != nil {
		return result, err
	}
	// Disk publication committed. No cancellation or fallible work may intervene.
	s.historyMu.Lock()
	for key, entries := range delta.entries {
		list := s.history.entries[key]
		list.entries = append(list.entries, entries.entries...)
	}
	s.historyMu.Unlock()
	return result, nil
}
func (s *DetachedMetadataStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
	err := s.closeRaw()
	if s.leases == 0 {
		s.history.release()
	}
	return err
}

type DetachedMetadataLease struct {
	mu    sync.Mutex
	store *DetachedMetadataStore
	raw   *disk.DetachedRawLease
	// Instance seam preserves the real lease and retirement path in tests.
	closeRaw func() error
	closed   bool
}

func (l *DetachedMetadataLease) Bounds() disk.DetachedRawBounds { return l.raw.Bounds() }
func (l *DetachedMetadataLease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.closeRaw()
	if l.closed {
		return err
	}
	l.closed = true
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases--
	if s.retired && s.leases == 0 {
		s.history.release()
	}
	return err
}
func (l *DetachedMetadataLease) visible(m types.Message, resolver TransactionDecisionResolver) bool {
	key := messageTransactionMarkerKey(m)
	s := l.store
	s.historyMu.RLock()
	var entries []transactionMarkerInfo
	if list := s.history.entries[key]; list != nil {
		entries = list.entries
	}
	index := sort.Search(len(entries), func(i int) bool { return entries[i].offset >= l.Bounds().EndOffset })
	var marker transactionMarkerInfo
	if index > 0 {
		marker = entries[index-1]
	}
	s.historyMu.RUnlock()
	return isReadCommittedVisibleWithMarker(m, l.Bounds().EndOffset, marker, index > 0, resolver)
}

// ReadCommittedMetadata evaluates the supplied receiver decision on every read.
// Pages contain at most 1024 visible records or 1 MiB of serialized payloads; a
// first oversized visible record is returned alone (up to disk.MaxMessageSize).
// Continue from the last returned offset + 1. Filtered intervals are scanned to
// the fixed end, and short byte-limited pages may require decoding them again.
func (l *DetachedMetadataLease) ReadCommittedMetadata(ctx context.Context, offset uint64, maxRecords int, resolver TransactionDecisionResolver) ([]types.Message, error) {
	return l.readMetadata(ctx, offset, maxRecords, resolver, false)
}

// ReadRawMetadata exposes the immutable, already-synced adopted prefix for
// history certification. Invisible intents and controls must not be omitted.
// It shares the same scratch budget and actual-work accounting as replay.
func (l *DetachedMetadataLease) ReadRawMetadata(ctx context.Context, offset uint64, maxRecords int) ([]types.Message, error) {
	return l.readMetadata(ctx, offset, maxRecords, nil, true)
}

// ReadCleanupPage reuses the certified raw store and its decode reservation.
// Remote cleanup never uses transaction-filtered omissions as lifecycle proof.
func (l *DetachedMetadataLease) ReadCleanupPage(ctx context.Context, offset uint64, maxRecords, maxBytes int) (page types.ReplayPage, resultErr error) {
	if ctx == nil {
		return page, fmt.Errorf("detached cleanup requires context")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return types.ReplayPage{}, disk.ErrDetachedBootstrapClosed
	}
	if err := l.store.budget.acquireScratch(ctx); err != nil {
		return types.ReplayPage{}, err
	}
	defer func() { <-l.store.budget.scratch }()
	before := l.raw.Work()
	defer func() {
		after := l.raw.Work()
		work := disk.DetachedRawWork{ReadBytes: after.ReadBytes - before.ReadBytes, IndexReadBytes: after.IndexReadBytes - before.IndexReadBytes, DecodedRecords: after.DecodedRecords - before.DecodedRecords}
		if n, ok := util.SafeUint64ToInt(work.ReadBytes + work.IndexReadBytes); ok {
			page.ReadBytes = n
		}
		l.store.workMu.Lock()
		addDetachedRawWork(&l.store.work.Reads, work)
		l.store.workMu.Unlock()
		l.store.budget.addWork(work)
	}()
	raw, err := l.raw.ReadRaw(ctx, offset, maxRecords, maxBytes)
	if errors.Is(err, disk.ErrRetainedPageLimit) {
		probe, probeErr := readDetachedOne(ctx, l.raw, offset)
		if probeErr != nil {
			return types.ReplayPage{}, probeErr
		}
		return types.ReplayPage{Next: offset}, &types.ReplayByteBudget{Size: probe.DecodedBytes, Budget: maxBytes}
	}
	if err != nil {
		return types.ReplayPage{}, err
	}
	page = types.ReplayPage{Next: raw.NextOffset, Records: len(raw.Messages), Bytes: raw.DecodedBytes,
		Cursor: types.ReplayCursor{Initialized: true}}
	for _, m := range raw.Messages {
		page.Messages = append(page.Messages, detachedMetadataMessage(m))
	}
	return page, nil
}

func (l *DetachedMetadataLease) readMetadata(ctx context.Context, offset uint64, maxRecords int, resolver TransactionDecisionResolver, rawRead bool) ([]types.Message, error) {
	if ctx == nil {
		return nil, fmt.Errorf("detached read requires context")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, disk.ErrDetachedBootstrapClosed
	}
	bounds := l.Bounds()
	if maxRecords <= 0 || offset < bounds.StartOffset || offset > bounds.EndOffset {
		return nil, fmt.Errorf("invalid detached metadata read bounds or limit")
	}
	if err := l.store.budget.acquireScratch(ctx); err != nil {
		return nil, err
	}
	defer func() { <-l.store.budget.scratch }()
	before := l.raw.Work()
	defer func() {
		after := l.raw.Work()
		l.store.workMu.Lock()
		work := disk.DetachedRawWork{ReadBytes: after.ReadBytes - before.ReadBytes, IndexReadBytes: after.IndexReadBytes - before.IndexReadBytes, DecodedRecords: after.DecodedRecords - before.DecodedRecords}
		addDetachedRawWork(&l.store.work.Reads, work)
		l.store.budget.addWork(work)
		l.store.workMu.Unlock()
	}()
	var out []types.Message
	total := 0
	for offset < bounds.EndOffset {
		page, err := readDetachedOne(ctx, l.raw, offset)
		if err != nil {
			return nil, err
		}
		for _, raw := range page.Messages {
			msg := detachedMetadataMessage(raw)
			visible := rawRead || l.visible(msg, resolver)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !visible {
				continue
			}
			if len(out) > 0 && page.DecodedBytes > detachedMetadataPageBytes-total {
				return out, nil
			}
			out = append(out, msg)
			total += page.DecodedBytes
			if total >= detachedMetadataPageBytes || len(out) >= min(maxRecords, detachedMetadataPageRecords) {
				return out, nil
			}
		}
		if page.Exhausted {
			break
		}
		if page.NextOffset <= offset {
			return nil, fmt.Errorf("detached metadata scan made no progress")
		}
		offset = page.NextOffset
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func detachedMetadataMessage(m types.DiskMessage) types.Message {
	return types.Message{
		Topic: m.Topic, Partition: int(m.Partition), Offset: m.Offset, Payload: m.Payload,
		ProducerID: m.ProducerID, SeqNum: m.SeqNum, Epoch: m.Epoch, LeaderEpoch: m.LeaderEpoch, AppendTimeMS: m.AppendTimeMS, Key: m.Key,
		EventType: m.EventType, SchemaVersion: m.SchemaVersion, AggregateVersion: m.AggregateVersion,
		Metadata: m.Metadata, EventID: m.EventID, PayloadDigest: m.PayloadDigest,
		TransactionalID: m.TransactionalID, TransactionState: m.TransactionState, TransactionMarker: m.TransactionMarker,
		ControlBatchType: m.ControlBatchType, ControlBatchVersion: m.ControlBatchVersion,
		ControlBatchCoordinatorEpoch: m.ControlBatchCoordinatorEpoch, ControlBatchKey: m.ControlBatchKey, ControlBatchValue: m.ControlBatchValue,
	}
}
