package replication

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
)

// A DNS address can be reassigned while its old TCP connection remains pooled.
// Count evidence only from the requested voter, never from the endpoint alone.
// RPCHeader.ID is already present in the current Raft protocol; no wire changes.
type identityRaftTransport struct{ *raft.NetworkTransport }

// checkRaftResponseID rejects evidence from an empty or different voter identity.
func checkRaftResponseID(id raft.ServerID, header raft.RPCHeader) error {
	if id == "" || string(header.ID) != string(id) {
		return fmt.Errorf("raft response identity mismatch: requested=%q received=%q", id, header.ID)
	}
	return nil
}

// checked preserves transport errors and evicts pooled streams after a voter mismatch.
func (t *identityRaftTransport) checked(id raft.ServerID, header raft.RPCHeader, err error) error {
	if err != nil {
		return err
	}
	if err = checkRaftResponseID(id, header); err != nil {
		// The underlying RPC has already put its connection back into the pool.
		// Drop it so the retry resolves DNS again instead of reusing a wrong peer.
		t.CloseStreams()
	}
	return err
}

// AppendEntries wraps append completion so evidence is checked against the requested voter.
func (t *identityRaftTransport) AppendEntries(id raft.ServerID, addr raft.ServerAddress, req *raft.AppendEntriesRequest, res *raft.AppendEntriesResponse) error {
	err := t.NetworkTransport.AppendEntries(id, addr, req, res)
	return t.checked(id, res.RPCHeader, err)
}

// RequestVote validates the responding voter before exposing its vote.
func (t *identityRaftTransport) RequestVote(id raft.ServerID, addr raft.ServerAddress, req *raft.RequestVoteRequest, res *raft.RequestVoteResponse) error {
	err := t.NetworkTransport.RequestVote(id, addr, req, res)
	return t.checked(id, res.RPCHeader, err)
}

// RequestPreVote validates the responding voter before exposing its pre-vote.
func (t *identityRaftTransport) RequestPreVote(id raft.ServerID, addr raft.ServerAddress, req *raft.RequestPreVoteRequest, res *raft.RequestPreVoteResponse) error {
	err := t.NetworkTransport.RequestPreVote(id, addr, req, res)
	return t.checked(id, res.RPCHeader, err)
}

// InstallSnapshot and TimeoutNow deliberately retain NetworkTransport behavior:
// Raft 1.7.3 sends no RPCHeader in those responses. A snapshot contains an already
// committed prefix; subsequent appends/votes still require the expected voter.

// AppendEntriesPipeline wraps a pipeline so both futures and completion notifications validate voter identity.
func (t *identityRaftTransport) AppendEntriesPipeline(id raft.ServerID, addr raft.ServerAddress) (raft.AppendPipeline, error) {
	inner, err := t.NetworkTransport.AppendEntriesPipeline(id, addr)
	if err != nil {
		return nil, err
	}
	p := &identityRaftPipeline{inner: inner, id: id, transport: t, completed: make(chan raft.AppendFuture), stop: make(chan struct{}), exited: make(chan struct{})}
	go p.relay()
	return p, nil
}

type identityRaftPipeline struct {
	inner                     raft.AppendPipeline
	id                        raft.ServerID
	transport                 *identityRaftTransport
	completed                 chan raft.AppendFuture
	stop, exited              chan struct{}
	closeOnce, invalidateOnce sync.Once
	closeErr                  error
	futureMu                  sync.Mutex
	futures                   map[raft.AppendFuture]*identityRaftFuture
}

// wrap shares one checked future between submission and completion, regardless of arrival order.
func (p *identityRaftPipeline) wrap(f raft.AppendFuture) *identityRaftFuture {
	// Each underlying future arrives exactly twice: once from AppendEntries
	// and once from Consumer, in either order. Share its sync.Once because
	// HashiCorp's Future.Error is not safe for concurrent calls.
	p.futureMu.Lock()
	defer p.futureMu.Unlock()
	if existing := p.futures[f]; existing != nil {
		delete(p.futures, f)
		return existing
	}
	if p.futures == nil {
		p.futures = make(map[raft.AppendFuture]*identityRaftFuture)
	}
	checked := &identityRaftFuture{AppendFuture: f, owner: p}
	p.futures[f] = checked
	return checked
}

// AppendEntries wraps append completion so evidence is checked against the requested voter.
func (p *identityRaftPipeline) AppendEntries(req *raft.AppendEntriesRequest, res *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	f, err := p.inner.AppendEntries(req, res)
	if err != nil {
		return nil, err
	}
	return p.wrap(f), nil
}

// Consumer returns the stream of identity-checked append completions.
func (p *identityRaftPipeline) Consumer() <-chan raft.AppendFuture { return p.completed }

// Close stops the pipeline and joins its relay exactly once.
func (p *identityRaftPipeline) Close() error {
	p.closeOnce.Do(func() { close(p.stop); p.closeErr = p.inner.Close(); <-p.exited })
	return p.closeErr
}

// relay validates completed futures before forwarding them, while allowing shutdown to interrupt delivery.
func (p *identityRaftPipeline) relay() {
	defer close(p.exited)
	for {
		select {
		case <-p.stop:
			return
		case f, ok := <-p.inner.Consumer():
			if !ok {
				return
			}
			checked := p.wrap(f)
			_ = checked.Error()
			select {
			case p.completed <- checked:
			case <-p.stop:
				return
			}
		}
	}
}

type identityRaftFuture struct {
	raft.AppendFuture
	owner    *identityRaftPipeline
	once     sync.Once
	err      error
	response raft.AppendEntriesResponse
}

// Error resolves the underlying future once and masks untrusted responses on failure.
func (f *identityRaftFuture) Error() error {
	f.once.Do(func() {
		f.err = f.AppendFuture.Error()
		f.response = *f.AppendFuture.Response()
		if f.err == nil {
			f.err = checkRaftResponseID(f.owner.id, f.response.RPCHeader)
			if f.err != nil {
				f.owner.invalidateOnce.Do(f.owner.transport.CloseStreams)
			}
		}
		if f.err != nil {
			// Raft 1.7.3 pipelineDecode reads Success without calling Future.Error.
			// Expose a failed copy; never count or trust the wrong member's term/log.
			f.response = raft.AppendEntriesResponse{Term: f.Request().Term}
		}
	})
	return f.err
}

// Response returns the validated response or a failed response that cannot count as replication evidence.
func (f *identityRaftFuture) Response() *raft.AppendEntriesResponse {
	_ = f.Error()
	return &f.response
}
