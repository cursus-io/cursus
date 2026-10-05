package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func startObservationRoutingServer(t *testing.T, handle func(*wire.Connection, wire.Frame) error) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer raw.Close()
				_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
				conn, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
				if err != nil {
					return
				}
				request, err := conn.ReadFrame()
				if err != nil {
					return
				}
				payload, err := wire.DecodeCommandPayload(request.Payload)
				if err != nil {
					return
				}
				if _, err := wire.RenderCommand(request.Command, payload); err != nil {
					return
				}
				_ = handle(conn, request)
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
	return listener.Addr().String()
}

func observationRoutingError(conn *wire.Connection, request wire.Frame, leader string, retryable, envelope bool) error {
	if envelope {
		payload, err := json.Marshal(map[string]string{"status": "ERROR", "error": fmt.Sprintf("NOT_LEADER leader=%s class=routing retryable=%t", leader, retryable)})
		if err != nil {
			return err
		}
		return writeWireTestResponse(conn, request, string(payload))
	}
	payload, err := wire.EncodeError(wire.ErrorPayload{Code: "NOT_LEADER", Class: wire.ErrorClassRouting, Retryable: retryable, Fields: map[string]string{"leader": leader}})
	if err != nil {
		return err
	}
	return conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusError, RequestID: request.RequestID, Payload: payload})
}

func TestObservationFollowsLeaderOutsideSeedList(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, envelope := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%t/envelope=%t", history, envelope), func(t *testing.T) {
				var leaderCalls, seedCalls atomic.Int32
				leader := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
					leaderCalls.Add(1)
					if err := writeWireTestResponse(conn, request, `{"status":"OK","completeness":"complete","has_more":false}`); err != nil {
						return err
					}
					batch, err := EncodeBatchMessages("orders", 0, "all", false, nil)
					if err != nil {
						return err
					}
					return conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: batch})
				})
				seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
					seedCalls.Add(1)
					return observationRoutingError(conn, request, leader, true, envelope)
				})
				client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{seed}, MaxRetries: 1, RequestTimeoutMS: 1000, RetryBackoffMS: 1})
				require.NoError(t, err)
				if history {
					_, err = client.ReadStreamHistory(context.Background(), HistoryRequest{Topic: "orders", Key: "order", FromVersion: 1, MaxRecords: 10, MaxBytes: 1024})
				} else {
					_, err = client.BrowseMessages(context.Background(), BrowseRequest{Topic: "orders", MaxRecords: 10, MaxBytes: 1024})
				}
				require.NoError(t, err)
				require.Equal(t, int32(1), leaderCalls.Load())
				require.Equal(t, int32(1), seedCalls.Load())
				require.Equal(t, []string{seed}, client.config.BrokerAddrs, "routing must not mutate shared seed configuration")
			})
		}
	}
}

func TestObservationRedirectsRespectRetryPolicy(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		t.Run(fmt.Sprintf("retryable=%t", retryable), func(t *testing.T) {
			var calls atomic.Int32
			unavailable, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			unavailableAddr := unavailable.Addr().String()
			require.NoError(t, unavailable.Close())
			seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
				calls.Add(1)
				return observationRoutingError(conn, request, unavailableAddr, retryable, true)
			})
			client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{seed}, MaxRetries: 2, RequestTimeoutMS: 100, RetryBackoffMS: 1})
			require.NoError(t, err)
			_, err = client.ReadStreamHistory(context.Background(), HistoryRequest{Topic: "orders", Key: "order", FromVersion: 1, MaxRecords: 10, MaxBytes: 1024})
			require.Error(t, err)
			if retryable {
				require.ErrorContains(t, err, "after 3 attempt(s)")
				require.Equal(t, int32(2), calls.Load(), "a failed redirected dial must fall back to the seeds")
			} else {
				require.Equal(t, int32(1), calls.Load())
			}
		})
	}
}

func TestObservationRedirectLoopIsBounded(t *testing.T) {
	var calls atomic.Int32
	var redirect atomic.Value
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		calls.Add(1)
		return observationRoutingError(conn, request, redirect.Load().(string), true, false)
	})
	redirect.Store(seed)
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{seed}, MaxRetries: 2, RequestTimeoutMS: 1000, RetryBackoffMS: 1})
	require.NoError(t, err)
	_, err = client.ReadStreamHistory(context.Background(), HistoryRequest{Topic: "orders", Key: "order", FromVersion: 1, MaxRecords: 10, MaxBytes: 1024})
	require.ErrorContains(t, err, "after 3 attempt(s)")
	require.Equal(t, int32(3), calls.Load())
}
