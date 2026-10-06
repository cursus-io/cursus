package sdk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestAdminAndObservationCancellationClosesBlockedRequest(t *testing.T) {
	for _, phase := range []string{"admin response", "capabilities", "envelope", "batch"} {
		t.Run(phase, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			reached := make(chan struct{})
			closed := make(chan error, 1)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					closed <- err
					return
				}
				defer func() { _ = raw.Close() }()
				_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
				conn, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
				if err != nil {
					closed <- err
					return
				}
				request, err := conn.ReadFrame()
				if err == nil && phase == "batch" {
					err = writeWireTestResponse(conn, request, `{"status":"OK","completeness":"complete","has_more":false}`)
				}
				if err != nil {
					closed <- err
					return
				}
				close(reached)
				_, err = raw.Read(make([]byte, 1))
				closed <- err
			}()
			client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{listener.Addr().String()}, RequestTimeoutMS: 5000})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				var err error
				switch phase {
				case "admin response":
					_, err = client.ListTopics(ctx)
				case "capabilities":
					_, err = client.Capabilities(ctx)
				default:
					_, err = client.ReadStreamHistory(ctx, HistoryRequest{Topic: "orders", Key: "order", FromVersion: 1, MaxRecords: 10, MaxBytes: 1024})
				}
				result <- err
			}()
			select {
			case <-reached:
			case err := <-closed:
				t.Fatalf("server failed before target phase: %v", err)
			case <-time.After(time.Second):
				t.Fatal("request did not reach target phase")
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("request waited for the configured timeout after cancellation")
			}
			select {
			case err := <-closed:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("server did not observe the cancelled socket close")
			}
		})
	}
}
