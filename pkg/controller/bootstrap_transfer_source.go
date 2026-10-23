package controller

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/recovery"
)

const bootstrapSourceLimit = 6
const bootstrapSourceCreateTimeout = 120 * time.Second
const bootstrapSourceTTL = 5 * time.Minute

// Process-wide, including separately configured command handlers. Source work
// never reserves receiver capacity. Disk independently accounts live bytes/debt.
var bootstrapSourcePermits = make(chan struct{}, bootstrapSourceLimit)

type bootstrapSourceExport interface {
	Manifest() disk.BootstrapMetadata
	ReadChunk(string, int64, int) ([]byte, error)
	Close() error
}
type bootstrapAttemptKey struct {
	requester fsm.BootstrapProcessIdentity
	attempt   string
}
type bootstrapPartitionKey struct {
	requester fsm.BootstrapProcessIdentity
	topic     string
	partition int
}
type bootstrapSourcePhase uint8

const (
	bootstrapCreating bootstrapSourcePhase = iota
	bootstrapReady
	bootstrapClosing
)

type bootstrapSourceAttempt struct {
	request                   fsm.BootstrapBeginRequest
	local                     fsm.BootstrapProcessIdentity
	key                       bootstrapAttemptKey
	partition                 bootstrapPartitionKey
	phase                     bootstrapSourcePhase
	createCtx                 context.Context
	cancelCreate              context.CancelFunc
	done, retire, pinsDrained chan struct{}
	pins                      int
	transferID                string
	response                  *fsm.BootstrapBeginResponse
	export                    bootstrapSourceExport
	expires                   time.Time
	hardExpires               time.Time
	activity                  chan struct{}
	err                       error
	prior                     *fsm.MetadataBootstrapContinuity
}
type bootstrapSourceCertificate struct {
	id         string
	continuity *fsm.MetadataBootstrapContinuity
	expires    time.Time
}
type bootstrapTransferSource struct {
	budget          *recovery.Budget
	incarnation     string
	mu              sync.Mutex
	state           *fsm.BrokerFSM
	identity        func() (fsm.BootstrapProcessIdentity, error)
	ctx             context.Context
	cancel          context.CancelFunc
	closed          bool
	owners          sync.WaitGroup
	attempts        map[bootstrapAttemptKey]*bootstrapSourceAttempt
	partitions      map[bootstrapPartitionKey]*bootstrapSourceAttempt
	transfers       map[string]*bootstrapSourceAttempt
	certificates    [128]*bootstrapSourceCertificate
	nextCertificate int
	cleanupErr      error
	// Immutable after construction/admission. Tests can wrap a real export to
	// pause non-preemptible I/O or simulate a completed cleanup-debt handoff.
	export         func(context.Context, *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error)
	fence          func(context.Context, *fsm.MetadataBootstrapSourceTicket, func(*fsm.MetadataBootstrapPublication) error) error
	replicaCapture func(context.Context, *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error)
	replicaFence   func(context.Context, *fsm.ReplicaBootstrapCapture, func(*fsm.ReplicaBootstrapPublication) error) error
	ttl            time.Duration
	maxLifetime    time.Duration
}

func newBootstrapTransferSource(ctx context.Context, state *fsm.BrokerFSM, identity func() (fsm.BootstrapProcessIdentity, error)) *bootstrapTransferSource {
	owner, cancel := context.WithCancel(ctx)
	s := &bootstrapTransferSource{state: state, identity: identity, ctx: owner, cancel: cancel, attempts: make(map[bootstrapAttemptKey]*bootstrapSourceAttempt), partitions: make(map[bootstrapPartitionKey]*bootstrapSourceAttempt), transfers: make(map[string]*bootstrapSourceAttempt), ttl: bootstrapSourceTTL}
	s.export = func(ctx context.Context, t *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		export, err := t.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		return export, nil
	}
	s.fence = state.WithMetadataBootstrapSourceFence
	s.replicaFence = state.WithReplicaBootstrapSourceFence
	// A canceled configured owner also retires idle ready exports. close waits
	// only for admitted owners, so this callback cannot join itself.
	context.AfterFunc(owner, func() { _ = s.close() })
	return s
}
func bootstrapContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("bootstrap transfer requires context")
	}
	return ctx.Err()
}
func (s *bootstrapTransferSource) requester(p fsm.BootstrapProcessIdentity) error {
	b := s.state.GetBroker(p.BrokerID)
	if b == nil || b.ID != p.BrokerID || b.Status != "active" || b.IncarnationID != p.IncarnationID || b.IncarnationEpoch != p.IncarnationEpoch || b.RecoveryProtocol < fsm.BootstrapRecoveryProtocolVersion {
		return fsm.ErrBootstrapTransferStaleFence
	}
	return nil
}

func (s *bootstrapTransferSource) begin(ctx context.Context, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	if err := bootstrapContext(ctx); err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	if err := r.Validate(); err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	if err := s.requester(r.Requester); err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	local, err := s.identity()
	if err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	if local != r.Source {
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferStaleFence
	}
	if err = s.requester(local); err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	key := bootstrapAttemptKey{r.Requester, r.AttemptID}
	partition := bootstrapPartitionKey{r.Requester, r.Topic, r.Partition}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferUnavailable
	}
	if e := s.attempts[key]; e != nil {
		s.mu.Unlock()
		if e.request != r {
			return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferAttemptConflict
		}
		return s.await(ctx, e)
	}
	if s.partitions[partition] != nil {
		s.mu.Unlock()
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferAttemptConflict
	}
	select {
	case bootstrapSourcePermits <- struct{}{}:
	default:
		s.mu.Unlock()
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferLimit
	}
	creating, cancel := context.WithTimeout(ctx, bootstrapSourceCreateTimeout)
	stop := context.AfterFunc(s.ctx, cancel)
	e := &bootstrapSourceAttempt{request: r, local: local, key: key, partition: partition, createCtx: creating, cancelCreate: func() { stop(); cancel() }, done: make(chan struct{}), retire: make(chan struct{}), pinsDrained: make(chan struct{}), activity: make(chan struct{}, 1)}
	if r.PriorCertificateID != "" {
		for _, c := range s.certificates {
			if c != nil && c.id == r.PriorCertificateID && time.Now().Before(c.expires) {
				e.prior = c.continuity
				break
			}
		}
	}
	s.attempts[key] = e
	s.partitions[partition] = e
	s.owners.Add(1) // Admission and close's closed flag share this mutex.
	s.mu.Unlock()
	go s.own(e)
	return s.await(ctx, e)
}
func (s *bootstrapTransferSource) await(ctx context.Context, e *bootstrapSourceAttempt) (fsm.BootstrapBeginResponse, error) {
	select {
	case <-ctx.Done():
		return fsm.BootstrapBeginResponse{}, ctx.Err()
	case <-e.done:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.err != nil {
		return fsm.BootstrapBeginResponse{}, e.err
	}
	if e.phase != bootstrapReady || !time.Now().Before(e.expires) {
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferNotFound
	}
	response := *e.response
	response.Manifest.Files = slices.Clone(response.Manifest.Files)
	return response, nil
}

// retireLocked only changes local state and closes notification channels. The
// caller invokes cancellation after unlocking; only the owner closes the export.
func (s *bootstrapTransferSource) retireLocked(e *bootstrapSourceAttempt) {
	if e.phase == bootstrapClosing {
		return
	}
	e.phase = bootstrapClosing
	close(e.retire)
	if e.pins == 0 {
		close(e.pinsDrained)
	}
}
func (s *bootstrapTransferSource) close() error {
	s.mu.Lock()
	s.closed = true
	entries := make([]*bootstrapSourceAttempt, 0, len(s.attempts))
	for _, e := range s.attempts {
		s.retireLocked(e)
		entries = append(entries, e)
	}
	s.mu.Unlock()
	s.cancel()
	for _, e := range entries {
		e.cancelCreate()
	}
	s.owners.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleanupErr
}

func (s *bootstrapTransferSource) release(ctx context.Context, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	if err := bootstrapContext(ctx); err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	if err := r.Validate(); err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	if err := s.requester(r.Requester); err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fsm.BootstrapReleaseResponse{}, fsm.ErrBootstrapTransferUnavailable
	}
	e := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}]
	if e != nil {
		if r.TransferID != "" && r.TransferID != e.transferID {
			s.mu.Unlock()
			return fsm.BootstrapReleaseResponse{}, fsm.ErrBootstrapTransferAttemptConflict
		}
		s.retireLocked(e)
	}
	s.mu.Unlock()
	if e != nil {
		e.cancelCreate()
	}
	return fsm.BootstrapReleaseResponse{Version: fsm.BootstrapTransferVersion, Request: r}, nil
}

func (s *bootstrapTransferSource) read(ctx context.Context, r fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
	if err := bootstrapContext(ctx); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	if err := r.Validate(); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	if err := s.requester(r.Requester); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	var transferred int64
	if s.budget != nil {
		key := r.Requester.BrokerID
		lease, err := s.budget.Acquire(ctx, key, int64(r.Size)*2+4096)
		if err != nil {
			return fsm.BootstrapChunkResponse{}, err
		}
		defer func() { recovery.SettleAfterResponse(ctx, func() { _ = lease.Finish(transferred) }) }()
	}
	s.mu.Lock()
	e := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}]
	if s.closed || e == nil || s.transfers[r.TransferID] != e || e.phase != bootstrapReady || !time.Now().Before(e.expires) {
		s.mu.Unlock()
		return fsm.BootstrapChunkResponse{}, fsm.ErrBootstrapTransferNotFound
	}
	size := int64(-1)
	for _, file := range e.response.Manifest.Files {
		if file.ID == r.FileID {
			size = file.Size
			break
		}
	}
	if size < 0 || r.Offset > size {
		s.mu.Unlock()
		return fsm.BootstrapChunkResponse{}, fmt.Errorf("bootstrap chunk outside file")
	}
	e.pins++
	export := e.export
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		e.pins--
		if e.pins == 0 && e.phase == bootstrapClosing {
			close(e.pinsDrained)
		}
		s.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	data, err := export.ReadChunk(r.FileID, r.Offset, r.Size)
	transferred = int64(len(data))*4/3 + 4096
	if err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	if err = ctx.Err(); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	response := fsm.BootstrapChunkResponse{Version: r.Version, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: r.TransferID, FileID: r.FileID, Offset: r.Offset, Data: data}
	if err = response.ValidateFor(r, size); err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	s.mu.Lock()
	s.touchReadyLocked(e, time.Now())
	s.mu.Unlock()
	return response, nil
}

// A progressing rate-limited transfer keeps its five-minute idle lease, but
// cannot extend the finite overall lifetime. Failed reads never renew it.
func (s *bootstrapTransferSource) touchReadyLocked(e *bootstrapSourceAttempt, now time.Time) {
	if e.phase != bootstrapReady || e.hardExpires.IsZero() || !now.Before(e.expires) {
		return
	}
	expires := now.Add(s.ttl)
	if e.hardExpires.Before(expires) {
		expires = e.hardExpires
	}
	if expires.After(e.expires) {
		e.expires = expires
		select {
		case e.activity <- struct{}{}:
		default:
		}
	}
}
