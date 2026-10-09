package sdk

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func writeEmptyStreamPage(conn *wire.Connection, request wire.Frame) error {
	if err := writeWireTestResponse(conn, request, `{"status":"OK","count":0,"has_more":false}`); err != nil {
		return err
	}
	batch, err := EncodeBatchMessages("state", 0, "all", false, nil)
	if err != nil {
		return err
	}
	return conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: batch})
}

func TestEventStoreReadStreamFollowsLeaderAndCachesConnection(t *testing.T) {
	var seedCalls, leaderCalls atomic.Int32
	leader := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		leaderCalls.Add(1)
		return writeEmptyStreamPage(conn, request)
	})
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		seedCalls.Add(1)
		return observationRoutingError(conn, request, leader, true, false)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	stream, err := store.ReadStream("saga-1")
	require.NoError(t, err)
	require.Empty(t, stream.Events)
	require.Equal(t, leader, store.addr)
	require.Equal(t, int32(1), seedCalls.Load())
	require.Equal(t, int32(1), leaderCalls.Load())
}

func TestEventStoreReadStreamRetriesClosedConnection(t *testing.T) {
	var calls atomic.Int32
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		if calls.Add(1) == 1 {
			return nil // A read has no side effects, so the whole stream may be retried.
		}
		return writeEmptyStreamPage(conn, request)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	_, err = store.ReadStream("saga-1")
	require.NoError(t, err)
	require.Equal(t, int32(2), calls.Load())
}

func TestEventStoreFallsBackToBootstrapAfterUnreachableLeaderHint(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := dead.Addr().String()
	require.NoError(t, dead.Close())
	var calls atomic.Int32
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		if calls.Add(1) == 1 {
			return observationRoutingError(conn, request, deadAddr, true, false)
		}
		return writeEmptyStreamPage(conn, request)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	_, err = store.ReadStream("saga-1")
	require.NoError(t, err)
	require.Equal(t, seed, store.addr)
	require.Equal(t, int32(2), calls.Load())
}

func TestEventStoreAppendRetriesOnlyExplicitLeaderRejection(t *testing.T) {
	var seedCalls, leaderCalls atomic.Int32
	leader := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		leaderCalls.Add(1)
		return writeWireTestResponse(conn, request, "OK version=1 offset=5 partition=0")
	})
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		seedCalls.Add(1)
		return observationRoutingError(conn, request, leader, true, false)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	result, err := store.AppendContext(context.Background(), "saga-1", 0, &Event{Type: "Started", Payload: "{}"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Version)
	require.Equal(t, int32(1), seedCalls.Load())
	require.Equal(t, int32(1), leaderCalls.Load())
}

func TestEventStoreAppendDoesNotRetryUnknownOutcome(t *testing.T) {
	var calls atomic.Int32
	seed := startObservationRoutingServer(t, func(_ *wire.Connection, _ wire.Frame) error {
		calls.Add(1)
		return nil // Close the connection after receiving the request, without an acknowledgement.
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	_, err = store.AppendContext(context.Background(), "saga-1", 0, &Event{Type: "Started", Payload: "{}"})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrRequestOutcomeUnknown))
	require.Equal(t, int32(1), calls.Load())
}

func TestEventStoreNonRetryableLeaderRejectionDoesNotRedirect(t *testing.T) {
	var calls atomic.Int32
	seed := startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		calls.Add(1)
		return observationRoutingError(conn, request, "127.0.0.1:1", false, false)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	_, err = store.ReadStream("saga-1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotLeader)
	require.Equal(t, int32(1), calls.Load())
}

func TestEventStoreBoundsRepeatedLeaderRedirects(t *testing.T) {
	var calls atomic.Int32
	var seed string
	seed = startObservationRoutingServer(t, func(conn *wire.Connection, request wire.Frame) error {
		calls.Add(1)
		return observationRoutingError(conn, request, seed, true, false)
	})
	store, err := NewEventStoreWithTimeout(seed, "state", "producer", time.Second)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	_, err = store.ReadStream("saga-1")
	require.ErrorIs(t, err, ErrNotLeader)
	require.Equal(t, int32(eventStoreMaxLeaderRedirects+1), calls.Load())
}
