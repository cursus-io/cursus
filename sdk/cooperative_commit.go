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
	c.commitPending = append(c.commitPending, r)
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
		// Collect for two milliseconds from the oldest queued request.
		// Time spent behind an in-flight append already counts toward this
		// window; do not sleep again when that append completes.
		c.commitMu.Lock()
		readyAt := c.commitPending[0].queued.Add(2 * time.Millisecond)
		c.commitMu.Unlock()
		if delay := time.Until(readyAt); delay > 0 {
			time.Sleep(delay)
		}
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
