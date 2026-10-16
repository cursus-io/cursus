package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func clientBootstrapRequest() fsm.BootstrapBeginRequest {
	return fsm.BootstrapBeginRequest{Version: 1, Purpose: "metadata", Topic: "__consumer_offsets", Leader: "source", LifecycleEpoch: 1, AttemptID: strings.Repeat("a", 32), Requester: fsm.BootstrapProcessIdentity{BrokerID: "requester", IncarnationID: "r", IncarnationEpoch: 1}, Source: fsm.BootstrapProcessIdentity{BrokerID: "source", IncarnationID: "s", IncarnationEpoch: 1}}
}
func clientBootstrapResponse(r fsm.BootstrapBeginRequest) fsm.BootstrapBeginResponse {
	return fsm.BootstrapBeginResponse{Version: 1, Request: r, CaptureMode: "full", CertificateID: strings.Repeat("b", 32), Manifest: disk.BootstrapMetadata{Version: disk.BootstrapVersion, TransferID: strings.Repeat("c", 32), Fence: disk.BootstrapFence{Topic: r.Topic, Partition: r.Partition, ReceiverID: r.Requester.BrokerID, SourceID: r.Source.BrokerID, LeaderID: r.Leader, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, CommittedHWM: r.FixedEnd, RecoveryProtocol: 2}, Files: []disk.BootstrapFile{{ID: "records", Role: "canonical-log", SHA256: strings.Repeat("a", 64)}, {ID: "catalog", Role: "snapshot-catalog", SHA256: strings.Repeat("b", 64)}}}}
}
func bootstrapPeer(t *testing.T, serve func(net.Conn)) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		serve(conn)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("peer did not finish")
		}
	})
	return ln.Addr().(*net.TCPAddr).Port
}
func bootstrapResponder(t *testing.T, respond func(wire.Frame) wire.Frame) int {
	t.Helper()
	return bootstrapPeer(t, func(conn net.Conn) {
		connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			return
		}
		request, err := connection.ReadFrame()
		if err != nil {
			return
		}
		_ = connection.WriteFrame(respond(request))
	})
}

func TestBootstrapClientBeginOutlivesOrdinaryDeadline(t *testing.T) {
	r := clientBootstrapRequest()
	encoded, err := json.Marshal(clientBootstrapResponse(r))
	require.NoError(t, err)
	port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
		time.Sleep(150 * time.Millisecond)
		return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: encoded}
	})
	c := NewSecureTCPClusterClient("secret", nil)
	c.timeout = 50 * time.Millisecond
	response, err := c.BeginBootstrapTransfer(context.Background(), "node@127.0.0.1:7000", port, r)
	require.NoError(t, err)
	require.NoError(t, response.ValidateFor(r))
}

func TestBootstrapClientReplicaPurposeResponse(t *testing.T) {
	for _, forbidden := range []bool{false, true} {
		t.Run(fmt.Sprint(forbidden), func(t *testing.T) {
			r := clientBootstrapRequest()
			r.Purpose = fsm.BootstrapPurposeReplica
			r.MinimumEnd = 1
			response := clientBootstrapResponse(r)
			response.CertificateID = ""
			response.Manifest.EndOffset, response.Manifest.Fence.CommittedHWM = 2, 2
			response.Manifest.Files[1].Size = 10
			encoded, err := json.Marshal(response)
			require.NoError(t, err)
			if forbidden {
				encoded = []byte(strings.Replace(string(encoded), `"minimum_end":1`, `"minimum_end":1,"fixed_end":0`, 1))
			}
			port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
				return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: encoded}
			})
			got, err := NewSecureTCPClusterClient("secret", nil).BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
			if forbidden {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, response, got)
			}
		})
	}
}

func TestBootstrapClientCancellationClosesSocketWithoutRelease(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			entered, closed := make(chan struct{}), make(chan struct{})
			port := bootstrapPeer(t, func(conn net.Conn) {
				connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
				if err != nil {
					return
				}
				_, err = connection.ReadFrame()
				if err != nil {
					return
				}
				close(entered)
				_, err = connection.ReadFrame()
				if err != nil {
					close(closed)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := NewSecureTCPClusterClient("secret", nil).BeginBootstrapTransfer(ctx, "127.0.0.1", port, clientBootstrapRequest())
				done <- err
			}()
			<-entered
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if deadline {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("caller cancellation did not return")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("cancel did not close private socket")
			}
		})
	}
}

func TestBootstrapClientHandshakeRemainsOrdinaryBounded(t *testing.T) {
	closed := make(chan struct{})
	port := bootstrapPeer(t, func(conn net.Conn) {
		buffer := make([]byte, 1024)
		for {
			if _, err := conn.Read(buffer); err != nil {
				close(closed)
				return
			}
		}
	})
	c := NewSecureTCPClusterClient("secret", nil)
	c.timeout = 50 * time.Millisecond
	start := time.Now()
	_, err := c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, clientBootstrapRequest())
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("handshake timeout left socket open")
	}
}

func TestBootstrapClientRejectsNilContext(t *testing.T) {
	c := NewSecureTCPClusterClient("secret", nil)
	_, err := c.BeginBootstrapTransfer(nil, "127.0.0.1", 1, clientBootstrapRequest()) //nolint:staticcheck // Verify rejection of an invalid nil caller context.
	require.ErrorContains(t, err, "context must not be nil")
}

func TestBootstrapClientRejectsMalformedAndUncorrelatedResponses(t *testing.T) {
	r := clientBootstrapRequest()
	good, err := json.Marshal(clientBootstrapResponse(r))
	require.NoError(t, err)
	for name, mutate := range map[string]func(*wire.Frame){
		"missing zero": func(f *wire.Frame) { f.Payload = []byte(strings.Replace(string(good), `"start_offset":0,`, "", 1)) },
		"unknown manifest": func(f *wire.Frame) {
			f.Payload = []byte(strings.Replace(string(good), `"manifest":{`, `"manifest":{"extra":0,`, 1))
		},
		"wrong manifest": func(f *wire.Frame) {
			f.Payload = []byte(strings.Replace(string(good), `"receiver_id":"requester"`, `"receiver_id":"other"`, 1))
		},
		"wrong request": func(f *wire.Frame) {
			f.Payload = []byte(strings.ReplaceAll(string(good), r.AttemptID, strings.Repeat("d", 32)))
		},
		"duplicate": func(f *wire.Frame) {
			f.Payload = []byte(strings.Replace(string(good), `"version":1`, `"version":1,"version":1`, 1))
		},
		"trailing":   func(f *wire.Frame) { f.Payload = append(good, []byte("{}")...) },
		"request ID": func(f *wire.Frame) { f.RequestID++ },
		"command":    func(f *wire.Frame) { f.Command = wire.CommandBootstrapRelease },
	} {
		t.Run(name, func(t *testing.T) {
			port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
				response := wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: good}
				mutate(&response)
				return response
			})
			_, err := NewSecureTCPClusterClient("secret", nil).BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
			require.Error(t, err)
			require.False(t, errors.Is(err, fsm.ErrBootstrapContinuityMiss))
		})
	}
}

func TestBootstrapClientChunkAndReleaseValidateCorrelation(t *testing.T) {
	r := clientBootstrapRequest()
	chunk := fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: strings.Repeat("c", 32), FileID: "records", Size: 2}
	for _, size := range []int{1, 2, 3, disk.BootstrapChunkBytes + 1, disk.BootstrapChunkBytes * 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			response := fsm.BootstrapChunkResponse{Version: 1, Requester: chunk.Requester, AttemptID: chunk.AttemptID, TransferID: chunk.TransferID, FileID: chunk.FileID, Data: make([]byte, size)}
			encoded, err := json.Marshal(response)
			require.NoError(t, err)
			port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
				return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: encoded}
			})
			_, err = NewSecureTCPClusterClient("secret", nil).ReadBootstrapTransferChunk(context.Background(), "127.0.0.1", port, chunk, 2)
			if size == 2 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	release := fsm.BootstrapReleaseRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID}
	for _, match := range []bool{true, false} {
		t.Run(map[bool]string{true: "release match", false: "release mismatch"}[match], func(t *testing.T) {
			response := fsm.BootstrapReleaseResponse{Version: 1, Request: release}
			if !match {
				response.Request.AttemptID = strings.Repeat("e", 32)
			}
			encoded, err := json.Marshal(response)
			require.NoError(t, err)
			port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
				return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: encoded}
			})
			_, err = NewSecureTCPClusterClient("secret", nil).ReleaseBootstrapTransfer(context.Background(), "127.0.0.1", port, release)
			if match {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBootstrapClientChunkAndReleaseKeepOrdinaryDeadline(t *testing.T) {
	r := clientBootstrapRequest()
	for _, command := range []wire.Command{wire.CommandBootstrapChunk, wire.CommandBootstrapRelease} {
		t.Run(command.String(), func(t *testing.T) {
			port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
				time.Sleep(150 * time.Millisecond)
				return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: []byte("{}")}
			})
			c := NewSecureTCPClusterClient("secret", nil)
			c.timeout = 50 * time.Millisecond
			var err error
			if command == wire.CommandBootstrapChunk {
				_, err = c.ReadBootstrapTransferChunk(context.Background(), "127.0.0.1", port, fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: r.AttemptID, FileID: "records", Size: 1}, 1)
			} else {
				_, err = c.ReleaseBootstrapTransfer(context.Background(), "127.0.0.1", port, fsm.BootstrapReleaseRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID})
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

func TestBootstrapClientChunkRejectsWrongFileIdentity(t *testing.T) {
	r := clientBootstrapRequest()
	chunk := fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: r.AttemptID, FileID: "records", Size: 1}
	response := fsm.BootstrapChunkResponse{Version: 1, Requester: chunk.Requester, AttemptID: chunk.AttemptID, TransferID: chunk.TransferID, FileID: "catalog", Data: []byte("x")}
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	port := bootstrapResponder(t, func(request wire.Frame) wire.Frame {
		return wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: encoded}
	})
	_, err = NewSecureTCPClusterClient("secret", nil).ReadBootstrapTransferChunk(context.Background(), "127.0.0.1", port, chunk, 1)
	require.ErrorContains(t, err, "identity mismatch")
}
