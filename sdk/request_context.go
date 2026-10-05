package sdk

import (
	"context"
	"fmt"
	"net"
	"time"
)

const defaultSDKRequestTimeout = 10 * time.Second

func boundedRequestContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultSDKRequestTimeout
	}
	deadline := time.Now().Add(timeout)
	if existing, ok := parent.Deadline(); ok && existing.Before(deadline) {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, deadline)
}

func bindConnectionToContext(ctx context.Context, conn net.Conn) (func(), error) {
	if ctx == nil || conn == nil {
		return nil, fmt.Errorf("request context and connection are required")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("request context must have a deadline")
	}
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set request deadline: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return func() {
		if !stop() || ctx.Err() != nil {
			_ = conn.Close()
			return
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			_ = conn.Close()
		}
	}, nil
}
