package server

import (
	"context"
	"errors"
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/util"
)

// Internal replication is already active, but the public listener and startup
// readiness remain unpublished. Let ISR catch up without losing the returning
// replica to a fatal startup exit or changing the durable prepared decision.
func awaitPreparedTransactionRecovery(ctx context.Context, distributed bool, recover func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := recover()
		if err == nil || !distributed || !onlyInsufficientISR(err) {
			return err
		}
		util.Warn("Prepared transaction startup waiting for ISR: %v", err)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
}

// Do not classify by substring or by just one member of an errors.Join tree:
// an unrelated persistence/fencing failure must still abort startup.
func onlyInsufficientISR(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyInsufficientISR(child) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyInsufficientISR(cause)
	}
	parsed, ok := protocol.ParseErrorResponse(err.Error())
	return ok && parsed.Code == "insufficient_in_sync_replicas" &&
		parsed.Class == protocol.ErrorClassAvailability && parsed.Retryable
}
