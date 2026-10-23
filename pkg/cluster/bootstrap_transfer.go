package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/wire"
)

const bootstrapBeginTimeout = 120 * time.Second

func (h *ClusterServer) handleBootstrapTransferContext(parent context.Context, conn net.Conn, command wire.Command, payload wire.CommandPayload, deadline time.Time) (any, *wire.ErrorPayload) {
	// Unlike legacy discovery commands, exports always require configured authentication.
	if len(h.authTokens) == 0 {
		return nil, &wire.ErrorPayload{Code: "cluster_unauthorized", Class: wire.ErrorClassAuthorization, Message: "bootstrap requires cluster authentication"}
	}
	if len(payload.Positionals) != 0 || len(payload.Fields) != 1 || payload.Fields["request"] == "" {
		return nil, validationError("bootstrap request field is required and must be the only field")
	}
	var request interface{ Validate() error }
	switch command {
	case wire.CommandBootstrapBegin:
		request = &fsm.BootstrapBeginRequest{}
	case wire.CommandBootstrapChunk:
		request = &fsm.BootstrapChunkRequest{}
	case wire.CommandBootstrapRelease:
		request = &fsm.BootstrapReleaseRequest{}
	}
	if err := json.Unmarshal([]byte(payload.Fields["request"]), request); err != nil {
		return nil, bootstrapValidationError(err)
	}
	if err := request.Validate(); err != nil {
		return nil, bootstrapValidationError(err)
	}
	source, ok := h.sd.(controller.BootstrapTransferSource)
	if !ok {
		return nil, bootstrapTransferError(fsm.ErrBootstrapTransferUnavailable)
	}
	// Only a fully decoded, authenticated Begin earns the capture deadline.
	if command == wire.CommandBootstrapBegin {
		deadline = time.Now().Add(bootstrapBeginTimeout)
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, internalClusterError(err)
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	var response any
	var err error
	switch r := request.(type) {
	case *fsm.BootstrapBeginRequest:
		response, err = source.BeginBootstrapTransfer(ctx, *r)
	case *fsm.BootstrapChunkRequest:
		response, err = source.ReadBootstrapTransferChunk(ctx, *r)
	case *fsm.BootstrapReleaseRequest:
		response, err = source.ReleaseBootstrapTransfer(ctx, *r)
	}
	if err != nil {
		return nil, bootstrapTransferError(err)
	}
	return response, nil
}

func bootstrapValidationError(err error) *wire.ErrorPayload {
	if errors.Is(err, fsm.ErrBootstrapTransferUnsupported) || errors.Is(err, fsm.ErrBootstrapTransferLimit) {
		return bootstrapTransferError(err)
	}
	return validationError(err.Error())
}

func bootstrapTransferError(err error) *wire.ErrorPayload {
	if errors.Is(err, recovery.ErrBusy) {
		return internalClusterError(err)
	}
	code, class := "bootstrap_internal", wire.ErrorClassInternal
	switch {
	case errors.Is(err, context.Canceled):
		code, class = "bootstrap_canceled", wire.ErrorClassAvailability
	case errors.Is(err, context.DeadlineExceeded):
		code, class = "bootstrap_deadline_exceeded", wire.ErrorClassAvailability
	case errors.Is(err, fsm.ErrBootstrapTransferUnavailable):
		code, class = "bootstrap_unavailable", wire.ErrorClassAvailability
	case errors.Is(err, fsm.ErrBootstrapTransferUnsupported):
		code, class = "bootstrap_unsupported", wire.ErrorClassValidation
	case errors.Is(err, fsm.ErrBootstrapTransferStaleFence):
		code, class = "bootstrap_stale_fence", wire.ErrorClassFencing
	case errors.Is(err, fsm.ErrBootstrapTransferAttemptConflict):
		code, class = "bootstrap_attempt_conflict", wire.ErrorClassConflict
	case errors.Is(err, fsm.ErrBootstrapTransferLimit):
		code, class = "bootstrap_limit", wire.ErrorClassAvailability
	case errors.Is(err, fsm.ErrBootstrapTransferNotFound):
		code, class = "bootstrap_not_found", wire.ErrorClassNotFound
	case errors.Is(err, fsm.ErrBootstrapContinuityMiss):
		code, class = "bootstrap_continuity_miss", wire.ErrorClassConflict
	}
	return &wire.ErrorPayload{Code: code, Class: class, Message: err.Error()}
}
