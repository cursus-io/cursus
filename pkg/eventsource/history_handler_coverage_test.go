package eventsource

import (
	"encoding/json"
	"net"
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func readStreamHistoryForTest(t *testing.T, h *Handler, cmd string) (map[string]any, *types.Batch) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	done := make(chan struct{})
	go func() {
		h.HandleReadStreamHistory(cmd, server)
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

func TestHandleReadStreamHistoryReturnsBoundedCommittedHistory(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	for version := 1; version <= 3; version++ {
		result := h.HandleAppendStream("APPEND_STREAM topic=orders key=order-history version=" + strconv.Itoa(version) + " event_type=OrderUpdated schema_version=1 message=event")
		require.Contains(t, result, "OK version=")
	}

	envelope, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=order-history from_version=1 max_records=2")
	require.Equal(t, "OK", envelope["status"])
	require.Equal(t, float64(3), envelope["head_version"])
	require.Equal(t, float64(3), envelope["next_version"])
	require.True(t, envelope["has_more"].(bool))
	require.Equal(t, "complete", envelope["completeness"])
	require.Len(t, batch.Messages, 2)
	require.Equal(t, uint64(1), batch.Messages[0].AggregateVersion)
	require.Equal(t, uint64(2), batch.Messages[1].AggregateVersion)
}

func TestHandleReadStreamHistoryRejectsInvalidRequests(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	for _, test := range []struct {
		name string
		cmd  string
		want string
	}{
		{"missing key", "READ_STREAM_HISTORY topic=orders", "missing_topic_or_key"},
		{"zero version", "READ_STREAM_HISTORY topic=orders key=order-history from_version=0", "invalid_from_version"},
		{"invalid records", "READ_STREAM_HISTORY topic=orders key=order-history max_records=0", "invalid_max_records"},
		{"invalid bytes", "READ_STREAM_HISTORY topic=orders key=order-history max_bytes=bad", "invalid_max_bytes"},
		{"plain topic", "READ_STREAM_HISTORY topic=plain key=order-history", "event_sourcing_not_available"},
		{"missing topic", "READ_STREAM_HISTORY topic=missing key=order-history", "event_sourcing_not_available"},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope, batch := readStreamHistoryForTest(t, h, test.cmd)
			require.Nil(t, batch)
			require.Equal(t, "ERROR", envelope["status"])
			require.Equal(t, test.want, envelope["error"])
		})
	}
}

func TestHandleReadStreamHistoryDetectsRangeGaps(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order-gap version=1 message=first"), "OK ")
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=order-gap version=2 message=second"), "OK ")

	envelope, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=order-gap from_version=1 to_version=2 max_bytes=1")
	require.Nil(t, batch)
	require.Equal(t, "ERROR", envelope["status"])
	require.Contains(t, envelope["error"], "record_too_large")
}

func TestHandleReadStreamHistoryResumesByteLimitedPages(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()
	for version := 1; version <= 3; version++ {
		require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=byte-page version="+strconv.Itoa(version)+" message=event"), "OK ")
	}
	// Each record costs 78 bytes under the history byte-budget contract.
	for version := 1; version <= 3; version++ {
		envelope, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=byte-page from_version="+strconv.Itoa(version)+" to_version=3 max_records=100 max_bytes=78")
		require.Equal(t, "complete", envelope["completeness"])
		require.Equal(t, version < 3, envelope["has_more"])
		require.Equal(t, float64(version+1), envelope["next_version"])
		require.Len(t, batch.Messages, 1)
		require.Equal(t, uint64(version), batch.Messages[0].AggregateVersion)
	}
}

func TestHandleReadStreamHistoryDistinguishesMissingIndexTailFromByteLimit(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()
	for version := 1; version <= 3; version++ {
		require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=missing-tail version="+strconv.Itoa(version)+" message=event"), "OK ")
	}
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	idx.mu.Lock()
	idx.entries["missing-tail"] = idx.entries["missing-tail"][:2]
	idx.mu.Unlock()
	first, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=missing-tail from_version=1 max_records=100 max_bytes=82")
	require.Len(t, batch.Messages, 1)
	require.Equal(t, "complete", first["completeness"])
	require.Equal(t, true, first["has_more"])
	last, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=missing-tail from_version=2 max_records=100 max_bytes=82")
	require.Len(t, batch.Messages, 1)
	require.Equal(t, uint64(2), batch.Messages[0].AggregateVersion)
	require.Equal(t, "partial", last["completeness"])
	require.Equal(t, false, last["has_more"])
}
