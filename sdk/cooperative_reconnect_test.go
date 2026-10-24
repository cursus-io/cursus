package sdk

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestCooperativeReconnectDoesNotWaitForCommitConnection(t *testing.T) {
	for _, mode := range []ConsumerMode{ModePolling, ModeStreaming} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			var offsetCalls atomic.Int32
			coordinatorAddr := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
				offsetCalls.Add(1)
				return writeWireTestResponse(conn, request, "OK offset=8 found=true")
			})
			cfg := NewDefaultConsumerConfig()
			cfg.Topic, cfg.GroupID = "orders", "workers"
			cfg.Mode, cfg.CoordinatorAddr = mode, coordinatorAddr
			cfg.PollInterval = time.Millisecond
			cfg.PollTimeoutMS, cfg.RequestTimeoutMS = 1000, 1000
			var c *CooperativeConsumer
			consumerReady := make(chan struct{})
			var requests atomic.Int32
			dataAddr := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
				<-consumerReady
				payload, err := wire.DecodeCommandPayload(request.Payload)
				if err != nil {
					return err
				}
				n := requests.Add(1)
				want := uint64(8 + 8*(n-1))
				if payload.Fields["offset"] != fmt.Sprint(want) {
					return fmt.Errorf("reconnect cursor = %s, want %d", payload.Fields["offset"], want)
				}
				if n == 1 {
					// Hold the operations connection before exposing the first page.
					// The next data connection must recover without waiting here.
					c.operationsRPC.gate <- struct{}{}
				}
				messages := make([]Message, 8)
				for i := range messages {
					messages[i] = Message{Offset: want + uint64(i), Partition: 0, Payload: "record"}
				}
				batch, err := EncodeBatchMessages("orders", 0, "all", false, messages)
				if err != nil {
					return err
				}
				return conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: batch})
				// The fixture closes this data connection after each page.
			})
			metadataAddr := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
				return writeWireTestResponse(conn, request, "OK leaders="+dataAddr)
			})
			cfg.BrokerAddrs = []string{metadataAddr}
			var err error
			c, err = NewCooperativeConsumer(cfg)
			close(consumerReady)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			batches := make(chan []Message)
			done := make(chan error, 1)
			go func() {
				done <- c.fetchPartition(ctx, &cooperativeWorker{token: cooperativeCommitRecord(0).Ownership}, batches)
			}()
			t.Cleanup(func() {
				cancel()
				c.transport.rootCancel()
				select {
				case <-c.operationsRPC.gate:
				default:
				}
				<-done
				c.operationsRPC.close()
			})
			for _, want := range []uint64{8, 16} {
				select {
				case messages := <-batches:
					require.Len(t, messages, 8)
					require.Equal(t, want, messages[0].Offset)
				case <-ctx.Done():
					t.Fatal("reconnect waited for the occupied commit connection")
				}
			}
			require.Equal(t, int32(1), offsetCalls.Load(), "only a new worker needs the initial committed position")
			require.GreaterOrEqual(t, c.Stats().FetchedRecords, uint64(16))
		})
	}
}
