package e2e_cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestAckFaultReplyProxy(t *testing.T) {
	for _, test := range []struct {
		name      string
		payload   string
		mismatch  bool
		wantError bool
	}{
		{"success_is_withheld", `{"status":"OK","last_offset":0}`, false, false},
		{"broker_failure_is_not_commit_evidence", `{"status":"ERROR"}`, false, true},
		{"wrong_request_is_not_commit_evidence", `{"status":"OK"}`, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = upstream.Close() })
			serverDone := make(chan error, 1)
			go func() {
				serverDone <- serveAckFaultProxyResponse(upstream, test.payload, test.mismatch)
			}()
			proxy, dropped := dropAckFaultReply(t, upstream.Addr().String())
			_, sent, err := ackFaultRoundTrip(proxy, "PUBLISH topic=test partition=0 acks=all message=value")
			require.True(t, sent, "request was not sent: %v", err)
			require.Error(t, err, "proxy must never synthesize or forward success to the client")
			select {
			case err := <-dropped:
				if test.wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("proxy did not finish")
			}
			select {
			case err := <-serverDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("upstream did not finish")
			}
		})
	}
}

func serveAckFaultProxyResponse(listener net.Listener, payload string, mismatch bool) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	server, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := server.ReadFrame()
	if err != nil {
		return err
	}
	if request.Command != wire.CommandPublish {
		return fmt.Errorf("unexpected command %s", request.Command)
	}
	if mismatch {
		request.RequestID++
	}
	return server.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command,
		RequestID: request.RequestID, Status: wire.StatusOK, Payload: []byte(payload)})
}
