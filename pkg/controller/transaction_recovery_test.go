package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func newDiskBackedTransactionHandler(t *testing.T) (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator, *disk.DiskManager) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.IndexSize = 1024
	cfg.DiskFlushIntervalMS = 1

	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	coord := coordinator.NewCoordinator(context.Background(), cfg, tm)
	ch := NewCommandHandler(tm, cfg, coord, nil, nil)

	t.Cleanup(func() {
		_ = ch.Close()
		dm.CloseAllHandlers()
	})

	return ch, tm, coord, dm
}

func prepareTransactionGroup(t *testing.T, tm *topic.TopicManager, coord *coordinator.Coordinator, topicName, groupName, memberID string) int {
	t.Helper()

	require.NoError(t, tm.CreateTopic(topicName, 1, false, false))
	require.NoError(t, coord.RegisterGroup(topicName, groupName, 1))
	_, err := coord.AddConsumer(groupName, memberID)
	require.NoError(t, err)
	return coord.GetGeneration(groupName)
}

func initAndStageTransaction(t *testing.T, ch *CommandHandler, txnID, topicName, groupName, memberID string, generation int, nextOffset uint64) (string, int64) {
	t.Helper()

	producerID, epoch, err := ch.TxnManager.InitProducer(txnID)
	require.NoError(t, err)
	require.NoError(t, ch.TxnManager.Begin(txnID, producerID, epoch))
	require.NoError(t, ch.TxnManager.AddMessage(txnID, producerID, epoch, transaction.MessageOperation{
		Topic:     topicName,
		Partition: 0,
		Message: types.Message{
			Payload:          fmt.Sprintf("payload-%s", txnID),
			ProducerID:       producerID,
			SeqNum:           1,
			Epoch:            epoch,
			TransactionalID:  txnID,
			TransactionState: types.TransactionStateOpen,
		},
	}))
	require.NoError(t, ch.TxnManager.AddOffsets(txnID, producerID, epoch, []transaction.OffsetOperation{{
		Topic:      topicName,
		Group:      groupName,
		Member:     memberID,
		Generation: generation,
		Partition:  0,
		Offset:     nextOffset,
	}}))
	return producerID, epoch
}

func readCommittedPayloads(t *testing.T, tm *topic.TopicManager, topicName string) []string {
	t.Helper()

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	p.FlushDisk()
	messages, err := p.ReadCommitted(0, 100)
	require.NoError(t, err)

	payloads := make([]string, 0, len(messages))
	for _, msg := range messages {
		payloads = append(payloads, msg.Payload)
	}
	return payloads
}

func TestRecoverPreparedTransactionsLeavesOpenTransactionsInvisible(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-open-recovery-topic"
	groupName := "txn-open-recovery-group"
	memberID := "txn-open-recovery-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	initAndStageTransaction(t, ch, "tx-open-recovery", topicName, groupName, memberID, generation, 7)

	require.NoError(t, ch.RecoverPreparedTransactions())

	tx, err := ch.TxnManager.Status("tx-open-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, tx.State)
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok := coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)
}

func TestRecoverPreparedTransactionsIsIdempotentAfterCommitWindow(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-commit-recovery-topic"
	groupName := "txn-commit-recovery-group"
	memberID := "txn-commit-recovery-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-commit-recovery", topicName, groupName, memberID, generation, 9)
	_, err := ch.TxnManager.PrepareCommit("tx-commit-recovery", producerID, epoch)
	require.NoError(t, err)

	require.NoError(t, ch.RecoverPreparedTransactions())

	tx, err := ch.TxnManager.Status("tx-commit-recovery")
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitted, tx.State)
	require.Equal(t, []string{"payload-tx-commit-recovery"}, readCommittedPayloads(t, tm, topicName))
	offset, ok := coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(9), offset)

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterRecovery := p.NextOffset()

	require.NoError(t, ch.RecoverPreparedTransactions())
	resp := ch.HandleCommand("END_TXN transactional_id=tx-commit-recovery producerId="+producerID+" epoch="+strconv.FormatInt(epoch, 10)+" result=commit", NewClientContext("", 0))
	require.True(t, strings.HasPrefix(resp, "OK "), resp)
	require.Equal(t, nextOffsetAfterRecovery, p.NextOffset())
	require.Equal(t, []string{"payload-tx-commit-recovery"}, readCommittedPayloads(t, tm, topicName))
	offset, ok = coord.GetOffset(groupName, topicName, 0)
	require.True(t, ok)
	require.Equal(t, uint64(9), offset)
}

func TestTransactionAbortRetryDoesNotMoveOffsetsOrAppendAgain(t *testing.T) {
	ch, tm, coord, _ := newDiskBackedTransactionHandler(t)
	topicName := "txn-abort-retry-topic"
	groupName := "txn-abort-retry-group"
	memberID := "txn-abort-retry-member"
	generation := prepareTransactionGroup(t, tm, coord, topicName, groupName, memberID)

	producerID, epoch := initAndStageTransaction(t, ch, "tx-abort-retry", topicName, groupName, memberID, generation, 11)
	ctx := NewClientContext("", 0)
	cmd := "END_TXN transactional_id=tx-abort-retry producerId=" + producerID + " epoch=" + strconv.FormatInt(epoch, 10) + " result=abort"

	resp := ch.HandleCommand(cmd, ctx)
	require.Contains(t, resp, "state=aborted")
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok := coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)

	p, err := tm.GetTopic(topicName).GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterAbort := p.NextOffset()

	resp = ch.HandleCommand(cmd, ctx)
	require.Contains(t, resp, "state=aborted")
	require.Equal(t, nextOffsetAfterAbort, p.NextOffset())
	require.Empty(t, readCommittedPayloads(t, tm, topicName))
	_, ok = coord.GetOffset(groupName, topicName, 0)
	require.False(t, ok)
}
