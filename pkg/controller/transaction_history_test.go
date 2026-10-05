package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestCommittedTransactionAppearsInBoundedBrowseAndStreamHistory(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("history-state", 1, false, true))
	ctx := NewClientContext("", 0)
	producer, epoch := initTransactionSession(t, ch, ctx, "history-txn")
	require.Contains(t, ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=history-txn producerId=%s epoch=%s", producer, epoch), ctx), "OK ")
	require.Contains(t, ch.HandleCommand(fmt.Sprintf("TXN_APPEND_STREAM transactional_id=history-txn topic=history-state key=run expected_version=1 producerId=%s seqNum=1 epoch=%s message=committed", producer, epoch), ctx), "OK ")
	require.Contains(t, ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=history-txn producerId=%s epoch=%s result=commit", producer, epoch), ctx), "OK ")
	p, err := tm.GetTopic("history-state").GetPartition(0)
	require.NoError(t, err)
	all, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, all, 1)
	bounded, err := p.ReadCommittedRange(all[0].Offset, all[0].Offset+1, 1)
	require.NoError(t, err)
	require.Equal(t, all, bounded)
	envelope, batch := browseMessagesForTest(t, ch, "BROWSE_MESSAGES topic=history-state partition=0 from_offset=0 to_offset=1")
	require.Equal(t, "OK", envelope["status"])
	require.Len(t, batch.Messages, 1)
	require.Equal(t, "committed", batch.Messages[0].Payload)

	client, server := net.Pipe()
	defer client.Close()
	require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		ch.ESHandler.HandleReadStreamHistory("READ_STREAM_HISTORY topic=history-state key=run from_version=1", server)
	}()
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &envelope))
	require.Equal(t, "complete", envelope["completeness"])
	require.Equal(t, false, envelope["has_more"])
	data, err = util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err = util.DecodeBatchMessages(data)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, "committed", batch.Messages[0].Payload)
	<-done
}
