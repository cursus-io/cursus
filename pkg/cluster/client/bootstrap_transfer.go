package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/wire"
)

const bootstrapBeginTimeout = 120 * time.Second

// BeginBootstrapTransfer performs one RPC. Delivery failure does not confer
// ownership of the shared attempt; only its owner may explicitly release it.
func (c *TCPClusterClient) BeginBootstrapTransfer(ctx context.Context, address string, port int, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	var response fsm.BootstrapBeginResponse
	if err := r.Validate(); err != nil {
		return response, err
	}
	data, err := c.sendBootstrapRequest(ctx, address, port, wire.CommandBootstrapBegin, r, bootstrapBeginTimeout)
	if err == nil {
		err = json.Unmarshal(data, &response)
	}
	if err == nil {
		err = response.ValidateFor(r)
	}
	if err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	return response, nil
}

func (c *TCPClusterClient) ReadBootstrapTransferChunk(ctx context.Context, address string, port int, r fsm.BootstrapChunkRequest, fileSize int64) (fsm.BootstrapChunkResponse, error) {
	var response fsm.BootstrapChunkResponse
	if err := r.Validate(); err != nil {
		return response, err
	}
	if fileSize < 0 || r.Offset > fileSize {
		return response, fmt.Errorf("bootstrap chunk outside file")
	}
	data, err := c.sendBootstrapRequest(ctx, address, port, wire.CommandBootstrapChunk, r, c.timeout)
	if err == nil {
		err = json.Unmarshal(data, &response)
	}
	if err == nil {
		err = response.ValidateFor(r, fileSize)
	}
	if err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	return response, nil
}

func (c *TCPClusterClient) ReleaseBootstrapTransfer(ctx context.Context, address string, port int, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	var response fsm.BootstrapReleaseResponse
	if err := r.Validate(); err != nil {
		return response, err
	}
	data, err := c.sendBootstrapRequest(ctx, address, port, wire.CommandBootstrapRelease, r, c.timeout)
	if err == nil {
		err = json.Unmarshal(data, &response)
	}
	if err == nil {
		err = response.ValidateFor(r)
	}
	if err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	return response, nil
}

func (c *TCPClusterClient) sendBootstrapRequest(parent context.Context, address string, port int, command wire.Command, request any, timeout time.Duration) (data []byte, err error) {
	if parent == nil {
		return nil, fmt.Errorf("bootstrap context must not be nil")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	// Prefer the caller's cancellation over the socket error caused by closing it.
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
			data = nil
		} else if deadline, ok := ctx.Deadline(); err != nil && ok && !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
			data = nil
		}
	}()
	ordinary, cancelOrdinary := context.WithTimeout(ctx, c.timeout)
	defer cancelOrdinary()
	conn, err := c.dialContext(ordinary, heartbeatTarget(address, port))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ordinary.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	connection, err := wire.ClientHandshake(recovery.MeterConnection(ctx, conn), []wire.Compression{wire.CompressionNone})
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: map[string]string{"auth_token": c.authToken, "request": string(encoded)}})
	if err != nil {
		return nil, err
	}
	id := atomic.AddUint64(&c.nextID, 1)
	if err = connection.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: command, RequestID: id, Payload: payload}); err != nil {
		return nil, err
	}
	deadline, _ = ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	response, err := connection.ReadFrameWithAdmission(func(encodedBytes, decodedBytes int) error {
		// Reject oversized chunk envelopes before allocating their payload. The
		// request bounds echoed identity fields; data is at most one base64 chunk.
		if command == wire.CommandBootstrapChunk {
			limit := len(encoded) + base64.StdEncoding.EncodedLen(disk.BootstrapChunkBytes) + 1024
			if encodedBytes > limit || decodedBytes > limit {
				return wire.ErrFrameTooLarge
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if response.Kind != wire.KindResponse || response.Command != command || response.RequestID != id {
		return nil, fmt.Errorf("bootstrap response correlation mismatch")
	}
	if response.Status == wire.StatusError {
		payload, decodeErr := wire.DecodeError(response.Payload)
		if decodeErr != nil {
			return nil, decodeErr
		}
		return nil, &bootstrapRPCError{wire.NewBrokerError(payload)}
	}
	if response.Status != wire.StatusOK {
		return nil, fmt.Errorf("unexpected bootstrap response status %d", response.Status)
	}
	return response.Payload, nil
}

type bootstrapRPCError struct{ broker *wire.BrokerError }

func (e *bootstrapRPCError) Error() string { return e.broker.Error() }
func (e *bootstrapRPCError) Unwrap() error { return e.broker }
func (e *bootstrapRPCError) Is(target error) bool {
	switch e.broker.Code {
	case "bootstrap_canceled":
		return target == context.Canceled
	case "bootstrap_deadline_exceeded":
		return target == context.DeadlineExceeded
	case "bootstrap_unavailable":
		return target == fsm.ErrBootstrapTransferUnavailable
	case "bootstrap_unsupported":
		return target == fsm.ErrBootstrapTransferUnsupported
	case "bootstrap_stale_fence":
		return target == fsm.ErrBootstrapTransferStaleFence
	case "bootstrap_attempt_conflict":
		return target == fsm.ErrBootstrapTransferAttemptConflict
	case "bootstrap_limit":
		return target == fsm.ErrBootstrapTransferLimit
	case "bootstrap_not_found":
		return target == fsm.ErrBootstrapTransferNotFound
	case "bootstrap_continuity_miss":
		return target == fsm.ErrBootstrapContinuityMiss
	}
	return false
}
