package sdk

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

type cooperativeCommitRequest struct {
	ctx    context.Context
	queued time.Time
	record CooperativeMessage
	done   chan error
}

// Each worker waits for this result before processing another batch. Coalescing
// changes the number of durable appends, not the per-partition replay window.
func (c *CooperativeConsumer) commitProcessedBatch(ctx context.Context, record CooperativeMessage) error {
	ctx, cancel := boundedRequestContext(ctx, c.transport.requestTimeout())
	defer cancel()
	started := time.Now()
	r := &cooperativeCommitRequest{ctx: ctx, queued: started, record: record, done: make(chan error, 1)}
	c.commitMu.Lock()
	if c.commitWake == nil {
		c.commitWake = make(chan struct{}, 1)
	}
	c.commitPending = append(c.commitPending, r)
	select {
	case c.commitWake <- struct{}{}:
	default:
	}
	if !c.commitRunning {
		c.commitRunning = true
		go c.flushProcessedBatches()
	}
	c.commitMu.Unlock()
	// Even on revoke, wait for the bounded submission to settle before closing
	// worker.done. A revocation ACK must not overtake an in-flight commit.
	err := <-r.done
	if elapsed := time.Since(started).Nanoseconds(); elapsed > 0 {
		c.commitWait.Add(uint64(elapsed))
	}
	return err
}

func (c *CooperativeConsumer) flushProcessedBatches() {
	for {
		c.waitForCommitBatch()
		c.commitMu.Lock()
		n := min(len(c.commitPending), protocol.MaxCooperativeCommitPartitions)
		batch := append([]*cooperativeCommitRequest(nil), c.commitPending[:n]...)
		clear(c.commitPending[:n])
		c.commitPending = c.commitPending[n:]
		c.commitMu.Unlock()
		active := batch[:0]
		for _, r := range batch {
			if err := r.ctx.Err(); err != nil {
				r.done <- err
			} else {
				active = append(active, r)
			}
		}
		if len(active) > 0 {
			c.flushCooperativeCommits(active)
		}
		c.commitMu.Lock()
		if len(c.commitPending) == 0 {
			c.commitPending = nil
			c.commitRunning = false
			c.commitMu.Unlock()
			return
		}
		c.commitMu.Unlock()
	}
}

// Keep the bounded collection window for staggered workers, but stop waiting
// when every running worker has submitted its completed batch. Worker counts
// are only a batching hint: tokens, offsets and durable acknowledgements still
// follow the same broker validation and per-worker barrier.
func (c *CooperativeConsumer) waitForCommitBatch() {
	c.commitMu.Lock()
	readyAt := c.commitPending[0].queued.Add(2 * time.Millisecond)
	wake := c.commitWake
	c.commitMu.Unlock()
	remaining := time.Until(readyAt)
	if remaining <= 0 {
		return
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	for {
		// Load stops first so concurrent worker completion cannot underflow.
		stopped, started := c.stops.Load(), c.starts.Load()
		workers := max(uint64(1), started-stopped)
		c.commitMu.Lock()
		full := uint64(len(c.commitPending)) >= min(workers, uint64(protocol.MaxCooperativeCommitPartitions))
		c.commitMu.Unlock()
		if full {
			return
		}
		select {
		case <-timer.C:
			return
		case <-wake:
		}
	}
}

func (c *CooperativeConsumer) flushCooperativeCommits(batch []*cooperativeCommitRequest) {
	sort.Slice(batch, func(i, j int) bool { return batch[i].record.Partition < batch[j].record.Partition })
	pairs := make([]wire.OffsetPair, len(batch))
	tokens := make([]protocol.CooperativeToken, len(batch))
	for i, r := range batch {
		pairs[i] = wire.OffsetPair{Partition: r.record.Partition, Offset: r.record.Offset + 1}
		tokens[i] = r.record.Ownership
	}
	encoded, err := wire.EncodeOffsetPairs(pairs)
	if err == nil {
		cfg := c.transport.config
		cmd := fmt.Sprintf("BATCH_COMMIT topic=%s group=%s member=%s offsets=%s ownerships=%s", cfg.Topic, cfg.GroupID, tokens[0].Grant.Member, encoded, protocol.EncodeCooperative(tokens))
		c.commitRequests.Add(1)
		c.commitPartitions.Add(uint64(len(batch)))
		// One cancelled partition cannot cancel other partitions' durable write.
		// Run cancellation and the command deadline still bound the request.
		_, err = c.command(c.transport.rootCtx, cmd, false)
	}
	var broker *BrokerError
	isolate := err != nil && (!cooperativeRetryable(err) || (errors.As(err, &broker) && broker.Code == "unstable_offset_commit"))
	for _, r := range batch {
		result := err
		if isolate && len(batch) > 1 {
			// The broker rejects the whole batch on a stale/reserved partition.
			// Isolate that failure so retained grants can still make progress.
			c.commitRequests.Add(1)
			c.commitPartitions.Add(1)
			result = c.Commit(r.ctx, r.record, r.record.Offset+1)
		}
		r.done <- result
	}
}
