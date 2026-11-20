package sdk

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ConsumerBatch is an ordered set of messages from one assigned partition.
// NextOffset is the durable offset to commit after the application completes
// its side effect.
type ConsumerBatch struct {
	Partition            int
	Messages             []Message
	NextOffset           uint64
	Generation           int64
	AssignmentGeneration uint64

	consumer *Consumer
	memberID string
}

// BatchHandler processes one ordered partition batch. The context is canceled
// when the assignment is lost or consumer shutdown begins.
type BatchHandler func(context.Context, ConsumerBatch) error

func (c *Consumer) makeConsumerBatch(pc *PartitionConsumer, messages []Message) ConsumerBatch {
	c.mu.RLock()
	generation, memberID := c.generation, c.memberID
	c.mu.RUnlock()
	return ConsumerBatch{
		Partition:            pc.partitionID,
		Messages:             append([]Message(nil), messages...),
		NextOffset:           messages[len(messages)-1].Offset + 1,
		Generation:           generation,
		AssignmentGeneration: pc.assignmentToken(),
		consumer:             c,
		memberID:             memberID,
	}
}

// CommitBatch commits batch.NextOffset if the consumer still owns the exact
// membership and local assignment that delivered the batch.
func (c *Consumer) CommitBatch(ctx context.Context, batch ConsumerBatch) error {
	if ctx == nil {
		return c.consumerCommitError(batch, fmt.Errorf("commit context must not be nil"))
	}
	if batch.consumer != c || batch.NextOffset == 0 || batch.AssignmentGeneration == 0 {
		return c.consumerCommitError(batch, ErrConsumerStaleGeneration)
	}
	if err := c.validateBatchOwnership(batch); err != nil {
		return c.consumerCommitError(batch, err)
	}

	requestCtx, cancelRequest := boundedRequestContext(ctx, c.requestTimeout())
	stopAssignmentCancel := context.AfterFunc(c.assignmentContext(), cancelRequest)
	defer func() {
		stopAssignmentCancel()
		cancelRequest()
	}()

	err := c.directCommitContext(
		requestCtx,
		batch.Partition,
		batch.NextOffset,
		batch.AssignmentGeneration,
		batch.Generation,
		batch.memberID,
	)
	if err != nil {
		cause := c.classifyBatchCommitError(batch, err)
		return c.consumerCommitError(batch, cause)
	}

	c.recordCommittedOffsets(map[int]uint64{batch.Partition: batch.NextOffset}, batch.AssignmentGeneration)
	return nil
}

func (c *Consumer) validateBatchOwnership(batch ConsumerBatch) error {
	if c.State() == ConsumerStateRebalancing {
		return ErrConsumerRebalancing
	}
	membership := c.Membership()
	if !membership.Ready || membership.Generation != batch.Generation ||
		membership.AssignmentGeneration != batch.AssignmentGeneration || membership.MemberID != batch.memberID {
		return ErrConsumerStaleGeneration
	}
	c.mu.RLock()
	pc := c.partitionConsumers[batch.Partition]
	c.mu.RUnlock()
	if pc == nil || pc.assignmentGeneration != batch.AssignmentGeneration {
		return ErrConsumerStaleGeneration
	}
	return nil
}

func (c *Consumer) classifyBatchCommitError(batch ConsumerBatch, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrConsumerRebalancing) {
		if c.State() == ConsumerStateRebalancing {
			return ErrConsumerRebalancing
		}
		return ErrConsumerStaleGeneration
	}
	var brokerErr *BrokerError
	if errors.As(err, &brokerErr) {
		switch strings.ToUpper(brokerErr.Code) {
		case "GEN_MISMATCH", "NOT_OWNER", "REBALANCE_REQUIRED", "MEMBER_NOT_FOUND":
			c.clearMembership()
			return errors.Join(ErrConsumerStaleGeneration, brokerErr)
		}
	}
	if ownershipErr := c.validateBatchOwnership(batch); ownershipErr != nil {
		return errors.Join(ownershipErr, err)
	}
	return err
}

func (c *Consumer) consumerCommitError(batch ConsumerBatch, cause error) error {
	c.mu.RLock()
	currentGeneration := c.generation
	c.mu.RUnlock()
	return &ConsumerCommitError{
		Partition:         batch.Partition,
		NextOffset:        batch.NextOffset,
		BatchGeneration:   batch.Generation,
		CurrentGeneration: currentGeneration,
		Cause:             cause,
	}
}
