package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func browseMessagesForTest(t *testing.T, ch *CommandHandler, cmd string) (map[string]any, *types.Batch) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	done := make(chan struct{})
	go func() {
		ch.HandleBrowseMessagesCommand(server, cmd)
		_ = server.Close()
		close(done)
	}()

	envelopeData, err := util.ReadWithLength(client)
	require.NoError(t, err)
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(envelopeData, &envelope))
	if envelope["status"] != "OK" {
		<-done
		return envelope, nil
	}
	batchData, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(batchData)
	require.NoError(t, err)
	<-done
	return envelope, batch
}

func TestHandleBrowseMessagesReturnsBoundedCommittedPage(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("browse-orders", 1, false, false))
	ctx := NewClientContext("", 0)
	for sequence, payload := range []string{"first", "second", "third"} {
		result := ch.HandleCommand(fmt.Sprintf("PUBLISH topic=browse-orders partition=0 producerId=browse seqNum=%d epoch=0 event_type=OrderUpdated schema_version=1 message=%s", sequence+1, payload), ctx)
		require.Contains(t, result, `"status":"OK"`)
	}

	envelope, batch := browseMessagesForTest(t, ch, "BROWSE_MESSAGES topic=browse-orders partition=0 from_offset=0 max_records=2 max_bytes=1048576")
	require.Equal(t, "OK", envelope["status"])
	require.Equal(t, float64(2), envelope["next_offset"])
	require.Equal(t, float64(3), envelope["readable_end_offset"])
	require.True(t, envelope["has_more"].(bool))
	require.Len(t, batch.Messages, 2)
	require.Equal(t, "first", batch.Messages[0].Payload)
	require.Equal(t, "second", batch.Messages[1].Payload)
}

func TestHandleBrowseMessagesRejectsInvalidRequests(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("browse-orders", 1, false, false))

	for _, test := range []struct {
		name string
		cmd  string
		want string
	}{
		{"missing topic", "BROWSE_MESSAGES partition=0 from_offset=0", "missing_topic"},
		{"missing partition", "BROWSE_MESSAGES topic=browse-orders from_offset=0", "missing_partition"},
		{"negative partition", "BROWSE_MESSAGES topic=browse-orders partition=-1 from_offset=0", "invalid_partition"},
		{"invalid offset", "BROWSE_MESSAGES topic=browse-orders partition=0 from_offset=bad", "invalid_from_offset"},
		{"invalid records", "BROWSE_MESSAGES topic=browse-orders partition=0 from_offset=0 max_records=0", "invalid_max_records"},
		{"invalid bytes", "BROWSE_MESSAGES topic=browse-orders partition=0 from_offset=0 max_bytes=bad", "invalid_max_bytes"},
		{"unknown topic", "BROWSE_MESSAGES topic=missing partition=0 from_offset=0", "topic_not_found topic=missing"},
		{"unknown partition", "BROWSE_MESSAGES topic=browse-orders partition=1 from_offset=0", "partition_not_found partition=1"},
		{"invalid upper bound", "BROWSE_MESSAGES topic=browse-orders partition=0 from_offset=2 to_offset=1", "invalid_to_offset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope, batch := browseMessagesForTest(t, ch, test.cmd)
			require.Nil(t, batch)
			require.Equal(t, "ERROR", envelope["status"])
			require.Equal(t, test.want, envelope["error"])
		})
	}
}

func TestHandleBrowseMessagesRejectsOversizedFirstRecord(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("browse-large", 1, false, false))
	result := ch.HandleCommand("PUBLISH topic=browse-large partition=0 producerId=browse seqNum=1 epoch=0 message=payload", NewClientContext("", 0))
	require.Contains(t, result, `"status":"OK"`)

	envelope, batch := browseMessagesForTest(t, ch, "BROWSE_MESSAGES topic=browse-large partition=0 from_offset=0 max_bytes=1")
	require.Nil(t, batch)
	require.Equal(t, "ERROR", envelope["status"])
	require.Contains(t, envelope["error"], "record_too_large offset=0")
}
