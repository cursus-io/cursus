package controller

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionalAppendStreamCommitsOnlyWithTransactionDecision(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("saga-state", 1, false, true))
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "saga-state-1")
	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=saga-state-1 producerId=%s epoch=%s", producerID, epoch), ctx), "OK "))
	appendCommand := fmt.Sprintf("TXN_APPEND_STREAM transactional_id=saga-state-1 topic=saga-state key=order:1:run-1 expected_version=1 producerId=%s seqNum=1 epoch=%s event_type=saga.transitioned schema_version=1 message={\"status\":\"RUNNING\"}", producerID, epoch)
	require.True(t, strings.HasPrefix(ch.HandleCommand(appendCommand, ctx), "OK "))

	partition, err := tm.GetTopic("saga-state").GetPartition(0)
	require.NoError(t, err)
	require.Greater(t, partition.NextOffset(), uint64(0), "open stream record must be durable before commit")
	require.Empty(t, readCommittedPayloads(t, tm, "saga-state"), "open stream record must remain invisible")

	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=saga-state-1 producerId=%s epoch=%s result=commit", producerID, epoch), ctx), "OK "))
	require.Equal(t, []string{"{\"status\":\"RUNNING\"}"}, readCommittedPayloads(t, tm, "saga-state"))
	require.Equal(t, "OK version=1", ch.HandleCommand("STREAM_VERSION topic=saga-state key=order:1:run-1", ctx))
}

func TestTransactionalAppendStreamRetriesAcceptedRequest(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("saga-state-retry", 1, false, true))
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "saga-state-retry-1")
	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=saga-state-retry-1 producerId=%s epoch=%s", producerID, epoch), ctx), "OK "))
	appendCommand := fmt.Sprintf("TXN_APPEND_STREAM transactional_id=saga-state-retry-1 topic=saga-state-retry key=order:retry:run-1 expected_version=1 producerId=%s seqNum=1 epoch=%s event_type=saga.transitioned message=running", producerID, epoch)

	require.True(t, strings.HasPrefix(ch.HandleCommand(appendCommand, ctx), "OK "))
	require.True(t, strings.HasPrefix(ch.HandleCommand(appendCommand, ctx), "OK "), "a retried append must reuse its broker sequence")
	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=saga-state-retry-1 producerId=%s epoch=%s result=commit", producerID, epoch), ctx), "OK "))
	require.Equal(t, []string{"running"}, readCommittedPayloads(t, tm, "saga-state-retry"))
}

func TestTransactionalAppendStreamReservesVersionUntilAbort(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("saga-state-reservation", 1, false, true))
	ctx := NewClientContext("", 0)

	firstProducer, firstEpoch := initTransactionSession(t, ch, ctx, "saga-reservation-1")
	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=saga-reservation-1 producerId=%s epoch=%s", firstProducer, firstEpoch), ctx), "OK "))
	firstAppend := fmt.Sprintf("TXN_APPEND_STREAM transactional_id=saga-reservation-1 topic=saga-state-reservation key=order:2:run-1 expected_version=1 producerId=%s seqNum=1 epoch=%s message=first", firstProducer, firstEpoch)
	require.True(t, strings.HasPrefix(ch.HandleCommand(firstAppend, ctx), "OK "))

	secondProducer, secondEpoch := initTransactionSession(t, ch, ctx, "saga-reservation-2")
	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=saga-reservation-2 producerId=%s epoch=%s", secondProducer, secondEpoch), ctx), "OK "))
	secondAppend := fmt.Sprintf("TXN_APPEND_STREAM transactional_id=saga-reservation-2 topic=saga-state-reservation key=order:2:run-1 expected_version=1 producerId=%s seqNum=1 epoch=%s message=second", secondProducer, secondEpoch)
	require.Contains(t, ch.HandleCommand(secondAppend, ctx), "stream version is reserved")
	require.Contains(t, ch.HandleCommand("APPEND_STREAM topic=saga-state-reservation key=order:2:run-1 version=1 message=normal", ctx), "stream_version_reserved")

	require.True(t, strings.HasPrefix(ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=saga-reservation-1 producerId=%s epoch=%s result=abort", firstProducer, firstEpoch), ctx), "OK "))
	require.True(t, strings.HasPrefix(ch.HandleCommand(secondAppend, ctx), "OK "), "abort must release the stream version reservation")
	transaction, err := ch.TxnManager.Status("saga-reservation-2")
	require.NoError(t, err)
	require.Len(t, transaction.Streams, 1)
	require.Equal(t, uint64(1), transaction.Streams[0].Message.SeqNum, "a rejected reservation must not consume the first partition sequence")
}
