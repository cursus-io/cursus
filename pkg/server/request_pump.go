package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

// requestPayloadBudget bounds encoded plus decoded request payloads across all
// connections, including reads in progress and requests executing in handlers.
const requestPayloadBudget = 256 * 1024 * 1024

var incomingPayloads = payloadBudget{limit: requestPayloadBudget}

type payloadBudget struct {
	mu          sync.Mutex
	used, limit int
}

func (b *payloadBudget) reserve(size int) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size < 0 || size > b.limit-b.used {
		return nil, fmt.Errorf("request payload budget exhausted")
	}
	b.used += size
	var once sync.Once
	return func() { once.Do(func() { b.mu.Lock(); b.used -= size; b.mu.Unlock() }) }, nil
}

type requestActivity struct {
	conn   net.Conn
	mu     sync.Mutex
	active bool
	last   time.Time
}

func (a *requestActivity) start() { a.mu.Lock(); a.active = true; a.mu.Unlock() }
func (a *requestActivity) finish() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active = false
	a.last = time.Now()
	// Wake an in-flight liveness read so it adopts the new idle deadline.
	if a.conn != nil {
		_ = a.conn.SetReadDeadline(a.last)
	}
}
func (a *requestActivity) deadline(idle time.Duration) time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active {
		return time.Now().Add(readDeadlinePoll)
	}
	return a.last.Add(idle)
}

// requestReader retries polling timeouts inside a frame, preserving partial
// bytes. Idle expiration applies only between completed requests.
type requestReader struct {
	net.Conn
	ctx         context.Context
	activity    *requestActivity
	idleTimeout time.Duration
}

func (r *requestReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		r.activity.mu.Lock()
		deadline := time.Now().Add(readDeadlinePoll)
		if !r.activity.active && r.activity.last.Add(r.idleTimeout).Before(deadline) {
			deadline = r.activity.last.Add(r.idleTimeout)
		}
		err := r.Conn.SetReadDeadline(deadline)
		r.activity.mu.Unlock()
		if err != nil {
			return 0, err
		}
		n, err := r.Conn.Read(p)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			if r.ctx.Err() != nil {
				return n, r.ctx.Err()
			}
			if time.Now().Before(r.activity.deadline(r.idleTimeout)) {
				if n > 0 {
					return n, nil
				}
				continue
			}
		}
		return n, err
	}
}

type admittedRequest struct {
	frame    wire.Frame
	finish   func()
	accepted chan struct{}
}

// pumpWireRequests reads at most one header ahead of the active request. The
// previous handler must finish before another payload can be allocated. STREAM
// additionally suspends header reads until registration either fails or cancels
// the pump as part of ownership transfer.
func pumpWireRequests(ctx context.Context, cancelConnection context.CancelFunc, connection *wire.Connection, activity *requestActivity, requests chan<- admittedRequest) {
	defer close(requests)
	previousDone := make(chan struct{})
	close(previousDone)
	for {
		var release func()
		request, err := readWireRequestWithAdmission(connection, func(encoded, decoded int) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-previousDone:
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			release, err = incomingPayloads.reserve(encoded + decoded)
			return err
		})
		if err != nil {
			if release != nil {
				release()
			}
			if ctx.Err() == nil {
				cancelConnection()
			}
			return
		}
		activity.start()
		done := make(chan struct{})
		var once sync.Once
		finish := func() { once.Do(func() { release(); activity.finish(); close(done) }) }
		accepted := make(chan struct{})
		select {
		case requests <- admittedRequest{frame: request, finish: finish, accepted: accepted}:
		case <-ctx.Done():
			finish()
			return
		}
		// Do not observe a post-frame EOF until the handler owns the complete
		// request. Otherwise disconnect cancellation can discard an accepted
		// fire-and-forget publish before it reaches processMessage.
		select {
		case <-accepted:
		case <-ctx.Done():
			finish()
			return
		}
		// Do not retain the dispatched payload while waiting on the next header.
		stream := request.Command == wire.CommandStream
		request = wire.Frame{}
		previousDone = done
		if stream {
			select {
			case <-ctx.Done():
				return
			case <-done:
			}
		}
	}
}
