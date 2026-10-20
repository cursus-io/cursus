package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func transferSourceFixture(t *testing.T) (*CommandHandler, *bootstrapTransferSource, fsm.BootstrapBeginRequest) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = .1
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	state := fsm.NewBrokerFSM(tm, nil)
	for _, id := range []string{"source", "requester"} {
		transferRegister(t, state, id, id+"-process", "active", 2)
	}
	def := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
	def.Partitions = 8
	def.ReplicationFactor = 1
	def.Policy = topic.ConsumerMetadataPolicy()
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &def, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 3, Data: append([]byte("TOPIC:"), payload...)}))
	h := newCoordinatorRoutingHandler("source", state, nil)
	h.TopicManager = tm
	require.NoError(t, h.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	t.Cleanup(func() { _ = h.Close(); tm.Stop(); dm.CloseAllHandlers() })
	r := fsm.BootstrapBeginRequest{Version: 1, Purpose: fsm.BootstrapPurposeMetadata, Topic: def.Name, Leader: "source", AttemptID: transferID(t), Source: transferIdentity(state.GetBroker("source")), Requester: transferIdentity(state.GetBroker("requester"))}
	meta := state.GetPartitionMetadata(def.Name + "-0")
	r.LeaderEpoch = meta.LeaderEpoch
	r.LifecycleEpoch = meta.LifecycleEpoch
	return h, h.bootstrapSource, r
}
func transferIdentity(b *fsm.BrokerInfo) fsm.BootstrapProcessIdentity {
	return fsm.BootstrapProcessIdentity{BrokerID: b.ID, IncarnationID: b.IncarnationID, IncarnationEpoch: b.IncarnationEpoch}
}
func transferRegister(t *testing.T, state *fsm.BrokerFSM, id, incarnation, status string, capability int) {
	t.Helper()
	data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: "127.0.0.1:7000", Status: status, IncarnationID: incarnation, LifecycleProtocol: 6, RecoveryProtocol: capability})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
}
func transferID(t *testing.T) string {
	t.Helper()
	id, err := fsm.NewBootstrapAttemptID()
	require.NoError(t, err)
	return id
}
func transferRelease(r fsm.BootstrapBeginRequest) fsm.BootstrapReleaseRequest {
	return fsm.BootstrapReleaseRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID}
}
func transferChunk(r fsm.BootstrapBeginResponse) fsm.BootstrapChunkRequest {
	return fsm.BootstrapChunkRequest{Version: 1, Requester: r.Request.Requester, AttemptID: r.Request.AttemptID, TransferID: r.Manifest.TransferID, FileID: "records", Size: disk.BootstrapChunkBytes}
}
func transferWait(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatal("transfer owner did not finish")
	}
}
func transferWaitEmpty(t *testing.T, s *bootstrapTransferSource) {
	t.Helper()
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.attempts) == 0 }, 5*time.Second, time.Millisecond)
}

type transferTestExport struct {
	bootstrapSourceExport
	read    func()
	data    func([]byte) []byte
	closeFn func() error
}

// The duplicate path first evaluates Done when it enters await. Observing this
// signal proves cancellation tests an admitted waiter, not the early Err guard.
type transferWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (ctx *transferWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

func (e *transferTestExport) ReadChunk(id string, offset int64, n int) ([]byte, error) {
	if e.read != nil {
		e.read()
	}
	data, err := e.bootstrapSourceExport.ReadChunk(id, offset, n)
	if e.data != nil {
		data = e.data(data)
	}
	return data, err
}
func (e *transferTestExport) Close() error {
	err := e.bootstrapSourceExport.Close()
	if e.closeFn != nil {
		err = errors.Join(err, e.closeFn())
	}
	return err
}

func TestBootstrapTransferSourceConcurrentAttempts(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var copies atomic.Int32
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		copies.Add(1)
		close(entered)
		<-resume
		return ticket.ExportContext(ctx)
	}
	const callers = 12
	results := make(chan fsm.BootstrapBeginResponse, callers)
	errs := make(chan error, callers)
	go func() { v, e := h.BeginBootstrapTransfer(context.Background(), r); results <- v; errs <- e }()
	transferWait(t, entered)
	for i := 1; i < callers; i++ {
		go func() { v, e := h.BeginBootstrapTransfer(context.Background(), r); results <- v; errs <- e }()
	}
	changed := r
	changed.FixedEnd++
	_, err := h.BeginBootstrapTransfer(context.Background(), changed)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
	changed = r
	changed.AttemptID = transferID(t)
	_, err = h.BeginBootstrapTransfer(context.Background(), changed)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
	ctx, cancel := context.WithCancel(context.Background())
	observed := &transferWaitContext{Context: ctx, observed: make(chan struct{})}
	duplicate := make(chan error, 1)
	go func() {
		_, err := h.BeginBootstrapTransfer(observed, r)
		duplicate <- err
	}()
	transferWait(t, observed.observed)
	cancel()
	require.ErrorIs(t, <-duplicate, context.Canceled)
	require.EqualValues(t, 1, copies.Load())
	close(resume)
	first := <-results
	for i := 0; i < callers; i++ {
		require.NoError(t, <-errs)
		if i > 0 {
			require.Equal(t, first, <-results)
		}
	}
	require.EqualValues(t, 1, copies.Load())
	require.NoError(t, first.ValidateFor(r))
	first.Manifest.Files[0].Size = 999
	again, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.NotEqualValues(t, 999, again.Manifest.Files[0].Size)
}

func TestBootstrapTransferSourceGlobalLimit(t *testing.T) {
	h1, s1, r1 := transferSourceFixture(t)
	h2, s2, r2 := transferSourceFixture(t)
	for i := 0; i < 6; i++ {
		h, r := h1, r1
		if i%2 == 1 {
			h, r = h2, r2
		}
		r.Partition = i
		r.AttemptID = transferID(t)
		_, err := h.BeginBootstrapTransfer(context.Background(), r)
		require.NoError(t, err)
	}
	r1.Partition = 7
	_, err := h1.BeginBootstrapTransfer(context.Background(), r1)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferLimit)
	require.NoError(t, h1.Close())
	transferWaitEmpty(t, s1)
	_, err = h2.BeginBootstrapTransfer(context.Background(), r2)
	require.NoError(t, err)
	require.NoError(t, h2.Close())
	transferWaitEmpty(t, s2)
}

func TestBootstrapTransferSourceCanceledCaptureRetainsOwnership(t *testing.T) {
	for _, how := range []string{"creator", "release", "close"} {
		t.Run(how, func(t *testing.T) {
			h, s, r := transferSourceFixture(t)
			entered, resume, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
				e, err := ticket.ExportContext(context.Background())
				close(entered)
				<-resume
				if err != nil {
					return nil, err
				}
				return &transferTestExport{bootstrapSourceExport: e, closeFn: func() error { close(closed); return nil }}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := h.BeginBootstrapTransfer(ctx, r); result <- err }()
			transferWait(t, entered)
			wrong := transferRelease(r)
			wrong.TransferID = transferID(t)
			_, err := h.ReleaseBootstrapTransfer(context.Background(), wrong)
			require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
			joined := make(chan struct{})
			switch how {
			case "creator":
				cancel()
			case "release":
				_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
				require.NoError(t, err)
			case "close":
				go func() { _ = h.Close(); close(joined) }()
				require.Eventually(t, func() bool {
					s.mu.Lock()
					defer s.mu.Unlock()
					return s.closed
				}, 5*time.Second, time.Millisecond)
			}
			s.mu.Lock()
			require.Len(t, s.attempts, 1)
			s.mu.Unlock()
			if how != "close" {
				other := r
				other.AttemptID = transferID(t)
				_, err = h.BeginBootstrapTransfer(context.Background(), other)
				require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
			}
			select {
			case <-closed:
				t.Fatal("closed live capture")
			default:
			}
			close(resume)
			require.Error(t, <-result)
			transferWait(t, closed)
			transferWaitEmpty(t, s)
			if how == "close" {
				transferWait(t, joined)
				_, err = h.BeginBootstrapTransfer(context.Background(), r)
				require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
			}
		})
	}
}

func TestBootstrapTransferSourcePinsAndRetirement(t *testing.T) {
	for _, how := range []string{"release", "expiry", "close"} {
		t.Run(how, func(t *testing.T) {
			h, s, r := transferSourceFixture(t)
			entered, resume, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			if how == "expiry" {
				s.ttl = 60 * time.Millisecond
			}
			s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
				e, err := ticket.ExportContext(ctx)
				if err != nil {
					return nil, err
				}
				return &transferTestExport{bootstrapSourceExport: e, read: func() { once.Do(func() { close(entered); <-resume }) }, closeFn: func() error { close(closed); return nil }}, nil
			}
			response, err := h.BeginBootstrapTransfer(context.Background(), r)
			require.NoError(t, err)
			read := make(chan error, 1)
			go func() { _, e := h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response)); read <- e }()
			transferWait(t, entered)
			joined := make(chan struct{})
			switch how {
			case "release":
				_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
				require.NoError(t, err)
			case "close":
				go func() { _ = h.Close(); close(joined) }()
				require.Eventually(t, func() bool {
					s.mu.Lock()
					defer s.mu.Unlock()
					return s.closed
				}, 5*time.Second, time.Millisecond)
			case "expiry":
				time.Sleep(100 * time.Millisecond)
			}
			select {
			case <-closed:
				t.Fatal("closed pinned export")
			default:
			}
			// Missing releases remain responsive while the owner joins a file read.
			missing := transferRelease(r)
			missing.AttemptID = transferID(t)
			_, err = h.ReleaseBootstrapTransfer(context.Background(), missing)
			if how != "close" {
				require.NoError(t, err)
			}
			close(resume)
			require.NoError(t, <-read)
			transferWait(t, closed)
			transferWaitEmpty(t, s)
			if how == "close" {
				transferWait(t, joined)
			}
		})
	}
}

func TestBootstrapTransferSourceReadyTTLAndCertificates(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	s.ttl = 100 * time.Millisecond
	entered, resume := make(chan struct{}), make(chan struct{})
	var copies atomic.Int32
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		if copies.Add(1) == 1 {
			close(entered)
			<-resume
		}
		return ticket.ExportContext(ctx)
	}
	results := make(chan fsm.BootstrapBeginResponse, 1)
	go func() { v, _ := h.BeginBootstrapTransfer(context.Background(), r); results <- v }()
	transferWait(t, entered)
	time.Sleep(120 * time.Millisecond)
	close(resume)
	first := <-results
	require.NotEmpty(t, first.CertificateID)
	s.mu.Lock()
	deadline := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}].expires
	s.mu.Unlock()
	require.WithinDuration(t, time.Now().Add(s.ttl), deadline, 50*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	_, err = h.ReadBootstrapTransferChunk(context.Background(), transferChunk(first))
	require.NoError(t, err)
	s.mu.Lock()
	require.Equal(t, deadline, s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}].expires)
	s.mu.Unlock()
	transferWaitEmpty(t, s)
	r.AttemptID = transferID(t)
	r.PriorCertificateID = first.CertificateID
	r.SuffixStart = r.FixedEnd
	// Certificate TTL is independent of export release, but expiry selects full.
	full, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureFull, full.CaptureMode)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	r.AttemptID = transferID(t)
	r.PriorCertificateID = full.CertificateID
	suffix, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureSuffix, suffix.CaptureMode)
	require.Zero(t, suffix.Manifest.Files[0].Size)
}

func TestBootstrapTransferSourceAuthorityAndConfiguration(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	require.Error(t, h.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	transferRegister(t, s.state, "requester", "replacement", "active", 2)
	_, err = h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response))
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	require.NoError(t, h.Close())
	require.Error(t, h.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	require.NoError(t, h.Close())
	bare := &CommandHandler{}
	_, err = bare.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
	require.NoError(t, bare.Close())
}

func TestBootstrapTransferSourcePublicationFailureCloses(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	closed := make(chan struct{})
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		transferRegister(t, s.state, "source", "replacement", "active", 2)
		return &transferTestExport{bootstrapSourceExport: e, closeFn: func() error { close(closed); return nil }}, nil
	}
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	transferWait(t, closed)
	transferWaitEmpty(t, s)
}

func TestBootstrapTransferSourceStorageErrorDoesNotFallback(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	var copies atomic.Int32
	storageErr := fmt.Errorf("corrupt retained records")
	s.export = func(context.Context, *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		copies.Add(1)
		return nil, storageErr
	}
	r.PriorCertificateID = transferID(t)
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, storageErr)
	require.EqualValues(t, 1, copies.Load())
	transferWaitEmpty(t, s)
}
