package replication

import (
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type identityTestStream struct {
	net.Listener
	accepts atomic.Int64
}

// Accept counts accepted connections to detect reuse of a misdirected stream.
func (s *identityTestStream) Accept() (net.Conn, error) {
	c, e := s.Listener.Accept()
	if e == nil {
		s.accepts.Add(1)
	}
	return c, e
}

// Dial connects the test transport to its requested peer address.
func (s *identityTestStream) Dial(a raft.ServerAddress, d time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(a), d)
}

// identityTestPeer serves Raft test RPCs with a mutable response identity.
func identityTestPeer(t *testing.T, id *atomic.Value) (*raft.NetworkTransport, *identityTestStream) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	s := &identityTestStream{Listener: l}
	peer := raft.NewNetworkTransport(s, 3, time.Second, io.Discard)
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case rpc := <-peer.Consumer():
				header := raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(id.Load().(string))}
				switch req := rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					rpc.Respond(&raft.AppendEntriesResponse{RPCHeader: header, Term: req.Term + 100, Success: true}, nil)
				case *raft.RequestVoteRequest:
					rpc.Respond(&raft.RequestVoteResponse{RPCHeader: header, Term: req.Term, Granted: true}, nil)
				case *raft.RequestPreVoteRequest:
					rpc.Respond(&raft.RequestPreVoteResponse{RPCHeader: header, Term: req.Term, Granted: true}, nil)
				case *raft.InstallSnapshotRequest:
					_, _ = io.Copy(io.Discard, rpc.Reader)
					// Match HashiCorp Raft 1.7.3's actual headerless responses.
					rpc.Respond(&raft.InstallSnapshotResponse{Term: req.Term, Success: true}, nil)
				case *raft.TimeoutNowRequest:
					rpc.Respond(&raft.TimeoutNowResponse{}, nil)
				}
			}
		}
	}()
	t.Cleanup(func() { _ = peer.Close(); close(stop); <-done })
	return peer, s
}

// TestRaftTransportRequiresTargetIdentity rejects wrong-voter responses on direct and pipelined RPCs.
func TestRaftTransportRequiresTargetIdentity(t *testing.T) {
	for _, id := range []string{"expected", "different", ""} {
		t.Run("response-"+id, func(t *testing.T) {
			var identity atomic.Value
			identity.Store(id)
			peer, _ := identityTestPeer(t, &identity)
			client, e := newRaftNetworkTransport(&config.Config{}, "127.0.0.1:0", "127.0.0.1:12345")
			require.NoError(t, e)
			t.Cleanup(func() { _ = client.Close() })
			header := raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("sender")}
			checks := map[string]func() error{
				"append": func() error {
					return client.AppendEntries("expected", peer.LocalAddr(), &raft.AppendEntriesRequest{RPCHeader: header, Term: 1}, &raft.AppendEntriesResponse{})
				},
				"vote": func() error {
					return client.RequestVote("expected", peer.LocalAddr(), &raft.RequestVoteRequest{RPCHeader: header, Term: 1}, &raft.RequestVoteResponse{})
				},
				"pre-vote": func() error {
					return client.RequestPreVote("expected", peer.LocalAddr(), &raft.RequestPreVoteRequest{RPCHeader: header, Term: 1}, &raft.RequestPreVoteResponse{})
				},
			}
			for name, call := range checks {
				t.Run(name, func(t *testing.T) {
					e := call()
					if id == "expected" {
						require.NoError(t, e)
					} else {
						require.Error(t, e)
					}
				})
			}
			t.Run("pipeline", func(t *testing.T) {
				p, e := client.AppendEntriesPipeline("expected", peer.LocalAddr())
				require.NoError(t, e)
				defer func() { require.NoError(t, p.Close()) }()
				f, e := p.AppendEntries(&raft.AppendEntriesRequest{RPCHeader: header, Term: 1}, &raft.AppendEntriesResponse{})
				require.NoError(t, e)
				e = f.Error()
				if id == "expected" {
					require.NoError(t, e)
					require.True(t, f.Response().Success)
				} else {
					require.Error(t, e)
					require.False(t, f.Response().Success)
					require.Equal(t, uint64(1), f.Response().Term, "a different peer cannot advance the term")
				}
				select {
				case f = <-p.Consumer():
					if id == "expected" {
						require.NoError(t, f.Error())
						require.True(t, f.Response().Success)
					} else {
						require.Error(t, f.Error())
						require.False(t, f.Response().Success, "Raft pipeline decoder ignores Future.Error")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("missing pipeline completion")
				}
			})
		})
	}
}

// TestRaftTransportPreservesHeaderlessSnapshotAndTransfer preserves the headerless snapshot and transfer responses used by Raft 1.7.3.
func TestRaftTransportPreservesHeaderlessSnapshotAndTransfer(t *testing.T) {
	var identity atomic.Value
	identity.Store("expected")
	peer, _ := identityTestPeer(t, &identity)
	client, err := newRaftNetworkTransport(&config.Config{}, "127.0.0.1:0", "127.0.0.1:12345")
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	header := raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("sender")}
	require.NoError(t, client.InstallSnapshot("expected", peer.LocalAddr(), &raft.InstallSnapshotRequest{RPCHeader: header, Term: 1, Size: 6}, &raft.InstallSnapshotResponse{}, strings.NewReader("prefix")))
	require.NoError(t, client.TimeoutNow("expected", peer.LocalAddr(), &raft.TimeoutNowRequest{RPCHeader: header}, &raft.TimeoutNowResponse{}))
}

// TestRaftTransportDropsMisdirectedPooledConnection requires a fresh connection after an address resolves to the wrong voter.
func TestRaftTransportDropsMisdirectedPooledConnection(t *testing.T) {
	var identity atomic.Value
	identity.Store("wrong")
	peer, stream := identityTestPeer(t, &identity)
	client, e := newRaftNetworkTransport(&config.Config{}, "127.0.0.1:0", "127.0.0.1:12345")
	require.NoError(t, e)
	defer func() { require.NoError(t, client.Close()) }()
	request := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax}, Term: 1}
	require.Error(t, client.AppendEntries("expected", peer.LocalAddr(), request, &raft.AppendEntriesResponse{}))
	require.Equal(t, int64(1), stream.accepts.Load())
	identity.Store("expected")
	require.NoError(t, client.AppendEntries("expected", peer.LocalAddr(), request, &raft.AppendEntriesResponse{}))
	require.Equal(t, int64(2), stream.accepts.Load(), "a rejected connection must not stay pooled")
	p, e := client.AppendEntriesPipeline("expected", peer.LocalAddr())
	require.NoError(t, e)
	_, e = p.AppendEntries(request, &raft.AppendEntriesResponse{})
	require.NoError(t, e)
	// Do not consume its completion: shutdown must release a blocked relay.
	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case e = <-done:
		require.NoError(t, e)
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline close blocked")
	}
}
