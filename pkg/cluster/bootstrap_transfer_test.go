package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func bootstrapRequest(t *testing.T) fsm.BootstrapBeginRequest {
	t.Helper()
	id, err := fsm.NewBootstrapAttemptID()
	require.NoError(t, err)
	return fsm.BootstrapBeginRequest{Version: 1, Purpose: "metadata", Topic: "__consumer_offsets", Leader: "source", LifecycleEpoch: 1, AttemptID: id,
		Requester: fsm.BootstrapProcessIdentity{BrokerID: "requester", IncarnationID: "r", IncarnationEpoch: 1}, Source: fsm.BootstrapProcessIdentity{BrokerID: "source", IncarnationID: "s", IncarnationEpoch: 1}}
}

type transportSource struct {
	begin func(context.Context, fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error)
	chunk func(context.Context, fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error)
}

func (s *transportSource) BeginBootstrapTransfer(ctx context.Context, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	return s.begin(ctx, r)
}
func (s *transportSource) ReadBootstrapTransferChunk(ctx context.Context, r fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
	return s.chunk(ctx, r)
}
func (s *transportSource) ReleaseBootstrapTransfer(_ context.Context, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	return fsm.BootstrapReleaseResponse{Version: 1, Request: r}, nil
}

func startBootstrapServer(t *testing.T, source controller.BootstrapTransferSource) (*ClusterServer, string, int) {
	t.Helper()
	sd := controller.NewServiceDiscoveryImpl(nil, "source", "", "")
	if source != nil {
		require.NoError(t, sd.BindBootstrapTransferSource(source))
	}
	s := NewSecureClusterServer(sd, "secret", nil)
	s.requestTimeout = 50 * time.Millisecond
	ln, err := s.Start("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, ln.Addr().String(), ln.Addr().(*net.TCPAddr).Port
}

func TestBootstrapTransportStrictRequests(t *testing.T) {
	_, addr, _ := startBootstrapServer(t, nil)
	r := bootstrapRequest(t)
	encoded, err := json.Marshal(r)
	require.NoError(t, err)
	for name, body := range map[string]string{
		"missing": "{}", "missing zero": strings.Replace(string(encoded), `"fixed_end":0,`, "", 1),
		"unknown":    strings.Replace(string(encoded), `"version":1`, `"version":1,"unexpected":0`, 1),
		"duplicate":  strings.Replace(string(encoded), `"version":1`, `"version":1,"version":1`, 1),
		"wrong case": strings.Replace(string(encoded), `"fixed_end"`, `"Fixed_end"`, 1),
		"trailing":   string(encoded) + "{}", "wrong purpose": strings.Replace(string(encoded), `"metadata"`, `"replica"`, 1),
		"version": strings.Replace(string(encoded), `"version":1`, `"version":2`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			response := clusterRoundTrip(t, addr, wire.CommandBootstrapBegin, map[string]string{"auth_token": "secret", "request": body})
			require.Equal(t, wire.StatusError, response.Status)
			payload, err := wire.DecodeError(response.Payload)
			require.NoError(t, err)
			require.NotEqual(t, "bootstrap_unavailable", payload.Code)
		})
	}
	response := clusterRoundTrip(t, addr, wire.CommandBootstrapBegin, map[string]string{"auth_token": "secret", "request": string(encoded)})
	payload, err := wire.DecodeError(response.Payload)
	require.NoError(t, err)
	require.Equal(t, "bootstrap_unavailable", payload.Code)
	r.Purpose = fsm.BootstrapPurposeReplica
	encoded, err = json.Marshal(r)
	require.NoError(t, err)
	response = clusterRoundTrip(t, addr, wire.CommandBootstrapBegin, map[string]string{"auth_token": "secret", "request": string(encoded)})
	payload, err = wire.DecodeError(response.Payload)
	require.NoError(t, err)
	require.Equal(t, "bootstrap_unavailable", payload.Code, "valid replica request reaches the configured-source boundary")
}

func TestBootstrapTransportTypedFailures(t *testing.T) {
	causes := []error{fsm.ErrBootstrapTransferUnavailable, fsm.ErrBootstrapTransferUnsupported, fsm.ErrBootstrapTransferStaleFence, fsm.ErrBootstrapTransferAttemptConflict, fsm.ErrBootstrapTransferLimit, fsm.ErrBootstrapTransferNotFound, fsm.ErrBootstrapContinuityMiss, context.Canceled, context.DeadlineExceeded, fmt.Errorf("storage checksum mismatch"), fmt.Errorf("read: disk I/O failure")}
	for _, cause := range causes {
		t.Run(cause.Error(), func(t *testing.T) {
			source := &transportSource{begin: func(context.Context, fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
				return fsm.BootstrapBeginResponse{}, fmt.Errorf("capture: %w", cause)
			}}
			_, _, port := startBootstrapServer(t, source)
			_, err := client.NewSecureTCPClusterClient("secret", nil).BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, bootstrapRequest(t))
			var broker *wire.BrokerError
			require.ErrorAs(t, err, &broker)
			if _, ok := cause.(fsm.BootstrapTransferError); ok || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
				require.ErrorIs(t, err, cause)
			} else {
				require.Equal(t, wire.ErrorClassInternal, broker.Class)
				require.False(t, errors.Is(err, fsm.ErrBootstrapContinuityMiss))
			}
		})
	}
}

func TestBootstrapBeginExtendsOnlyAuthenticatedValidRequest(t *testing.T) {
	source := &transportSource{begin: func(ctx context.Context, _ fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Greater(t, time.Until(deadline), time.Minute)
		time.Sleep(150 * time.Millisecond)
		return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferNotFound
	}}
	s, addr, port := startBootstrapServer(t, source)
	_, err := client.NewSecureTCPClusterClient("secret", nil).BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, bootstrapRequest(t))
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferNotFound)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	require.Eventually(t, func() bool { s.lifecycleMu.Lock(); defer s.lifecycleMu.Unlock(); return len(s.connections) == 0 }, time.Second, time.Millisecond)
}

func TestBootstrapRejectedCommandsNeverExtendDeadline(t *testing.T) {
	r := bootstrapRequest(t)
	encoded, err := json.Marshal(r)
	require.NoError(t, err)
	for _, test := range []struct {
		name, configured, token, body string
		command                       wire.Command
	}{
		{"unauthenticated", "secret", "wrong", string(encoded), wire.CommandBootstrapBegin},
		{"unconfigured authentication", "", "", string(encoded), wire.CommandBootstrapBegin},
		{"invalid request", "secret", "secret", "{}", wire.CommandBootstrapBegin},
		{"unsupported command", "secret", "secret", string(encoded), wire.CommandAuth},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := NewSecureClusterServer(nil, test.configured, nil)
			s.requestTimeout = 100 * time.Millisecond
			server, peer := net.Pipe()
			defer func() { _ = peer.Close(); _ = s.Close() }()
			deadlines := make(chan time.Time, 8)
			done := make(chan struct{})
			go func() { s.handleConnection(deadlineRecordingConn{Conn: server, deadlines: deadlines}); close(done) }()
			connection, err := wire.ClientHandshake(peer, []wire.Compression{wire.CompressionNone})
			require.NoError(t, err)
			body, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: map[string]string{"auth_token": test.token, "request": test.body}})
			require.NoError(t, err)
			require.NoError(t, connection.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: test.command, RequestID: 1, Payload: body}))
			response, err := connection.ReadFrame()
			require.NoError(t, err)
			require.Equal(t, wire.StatusError, response.Status)
			_ = peer.Close()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handler did not stop")
			}
			close(deadlines)
			for deadline := range deadlines {
				require.Less(t, time.Until(deadline), time.Second)
			}
		})
	}
}

func TestBootstrapCloseCancelsBeginAndJoinsNonpreemptibleRead(t *testing.T) {
	enteredBegin, enteredRead, resumeRead := make(chan struct{}), make(chan struct{}), make(chan struct{})
	source := &transportSource{
		begin: func(ctx context.Context, _ fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
			close(enteredBegin)
			<-ctx.Done()
			return fsm.BootstrapBeginResponse{}, ctx.Err()
		},
		chunk: func(context.Context, fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
			close(enteredRead)
			<-resumeRead
			return fsm.BootstrapChunkResponse{}, fsm.ErrBootstrapTransferNotFound
		},
	}
	s, _, port := startBootstrapServer(t, source)
	c := client.NewSecureTCPClusterClient("secret", nil)
	r := bootstrapRequest(t)
	beginDone, readDone := make(chan struct{}), make(chan struct{})
	go func() { _, _ = c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r); close(beginDone) }()
	<-enteredBegin
	chunk := fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: r.AttemptID, FileID: "records", Size: 1}
	go func() {
		_, _ = c.ReadBootstrapTransferChunk(context.Background(), "127.0.0.1", port, chunk, 1)
		close(readDone)
	}()
	<-enteredRead
	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-beginDone:
	case <-time.After(time.Second):
		t.Fatal("begin not canceled")
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("read socket not closed")
	}
	select {
	case <-closed:
		t.Fatal("close failed to join source read")
	case <-time.After(30 * time.Millisecond):
	}
	// The lifecycle lock remains available while Close waits on source I/O.
	_, err := s.Start("127.0.0.1:0")
	require.ErrorIs(t, err, net.ErrClosed)
	close(resumeRead)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not complete")
	}
}

func TestBootstrapCloseConcurrentAdmission(t *testing.T) {
	for range 20 {
		s, addr, _ := startBootstrapServer(t, nil)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				conn, err := net.DialTimeout("tcp", addr, time.Second)
				if err == nil {
					_ = conn.Close()
				}
			})
			wg.Go(func() { _ = s.Close() })
		}
		wg.Wait()
		s.lifecycleMu.Lock()
		require.Empty(t, s.connections)
		require.Empty(t, s.listeners)
		s.lifecycleMu.Unlock()
	}
}

func TestBootstrapTransportUnavailableAndAuthorization(t *testing.T) {
	sd := controller.NewServiceDiscovery(nil, "source", "", "")
	s := NewSecureClusterServer(sd, "secret", nil)
	ln, err := s.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	port := ln.Addr().(*net.TCPAddr).Port
	r := bootstrapRequest(t)
	for _, token := range []string{"secret", "wrong"} {
		_, err := client.NewSecureTCPClusterClient(token, nil).BeginBootstrapTransfer(context.Background(), "127.0.0.1:7000", port, r)
		var broker *wire.BrokerError
		require.ErrorAs(t, err, &broker)
		if token == "secret" {
			require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
		} else {
			require.Equal(t, "cluster_unauthorized", broker.Code)
			require.False(t, errors.Is(err, fsm.ErrBootstrapContinuityMiss))
		}
	}
}

func TestClusterCloseJoinsHandshakeAndRejectsRestart(t *testing.T) {
	s := NewClusterServer(nil)
	ln, err := s.Start("127.0.0.1:0")
	require.NoError(t, err)
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	done := make(chan struct{})
	go func() { _ = s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not join handshake")
	}
	require.NoError(t, s.Close())
	_, err = s.Start("127.0.0.1:0")
	require.ErrorIs(t, err, net.ErrClosed)
}
