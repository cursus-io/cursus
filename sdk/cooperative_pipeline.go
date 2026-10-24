package sdk

import (
	"context"
	"time"
)

// A worker owns both stages. The unbuffered handoff bounds read-ahead to one
// batch beyond the batch being processed; fetching never advances a commit.
func (c *CooperativeConsumer) runPartition(ctx context.Context, w *cooperativeWorker, handler func(context.Context, CooperativeMessage) error) error {
	fetchCtx, cancel := context.WithCancel(ctx)
	batches := make(chan []Message)
	done := make(chan struct{})
	var fetchErr error
	go func() {
		defer close(done)
		defer close(batches)
		fetchErr = c.fetchPartition(fetchCtx, w, batches)
	}()
	// Cancellation closes the fetch connection. Wait for the reader as well as
	// the handler/commit before worker.done can permit a revocation ACK.
	defer func() { cancel(); <-done }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages, ok := <-batches:
			if !ok {
				<-done
				return fetchErr
			}
			if err := c.processPartitionBatch(ctx, w, messages, handler); err != nil {
				return err
			}
		}
	}
}

func (c *CooperativeConsumer) processPartitionBatch(ctx context.Context, w *cooperativeWorker, messages []Message, handler func(context.Context, CooperativeMessage) error) error {
	cfg := c.transport.config
	pid := w.token.Grant.Partition.Partition
	for _, message := range messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		record := CooperativeMessage{Message: message, Group: cfg.GroupID, Ownership: w.token}
		backoff := newBackoff(cfg.HandlerRetryBackoff, cfg.HandlerRetryMaxBackoff)
		for attempt := 0; ; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			err := handler(ctx, record)
			if err == nil {
				break
			}
			if attempt >= cfg.HandlerMaxRetries {
				return &ConsumerHandlerError{Partition: pid, Offset: message.Offset, Attempts: attempt + 1, Cause: err}
			}
			if !cooperativeWait(ctx, backoff.duration()) {
				return ctx.Err()
			}
		}
		c.delivered.Add(1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if cfg.EnableAutoCommit && message.Offset == messages[len(messages)-1].Offset {
			commitRetry := newBackoff(max(cfg.CommitRetryBackoff, 100*time.Millisecond), time.Second)
			for {
				err := c.commitProcessedBatch(ctx, record)
				if err == nil {
					break
				}
				if !cooperativeRetryable(err) {
					return err
				}
				if !cooperativeWait(ctx, commitRetry.duration()) {
					return ctx.Err()
				}
			}
		}
	}
	return nil
}
