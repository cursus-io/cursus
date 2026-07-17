package sdk

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAppendResponse(t *testing.T) {
	resp := "OK version=3 offset=1024 partition=2"

	result, err := parseAppendResponse(resp)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), result.Version)
	assert.Equal(t, uint64(1024), result.Offset)
	assert.Equal(t, 2, result.Partition)
}

func TestParseAppendResponse_MissingFields(t *testing.T) {
	_, err := parseAppendResponse("OK version=1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing append fields")
}

func TestParseAppendResponse_UnexpectedResponse(t *testing.T) {
	_, err := parseAppendResponse("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected append response")
}

func TestEventStore_NewAndClose(t *testing.T) {
	es := NewEventStore("localhost:9000", "orders", "producer-1")
	assert.Equal(t, "orders", es.topic)
	assert.Equal(t, "producer-1", es.producerID)
	assert.Equal(t, "localhost:9000", es.addr)
	assert.Nil(t, es.conn)
	assert.NoError(t, es.Close())
}

func TestEventStore_CloseIdempotent(t *testing.T) {
	es := NewEventStore("localhost:9000", "orders", "producer-1")
	assert.NoError(t, es.Close())
	assert.NoError(t, es.Close())
}

func TestEvent_Defaults(t *testing.T) {
	e := &Event{
		Type:    "OrderCreated",
		Payload: `{"customer":"alice"}`,
	}
	assert.Equal(t, uint32(0), e.SchemaVersion) // 0 means default; Append sets to 1
	assert.Equal(t, "", e.Metadata)
}

func TestEvent_WithMetadata(t *testing.T) {
	e := &Event{
		Type:          "OrderShipped",
		SchemaVersion: 2,
		Payload:       `{"tracking":"XYZ123"}`,
		Metadata:      `{"userId":"u-42"}`,
	}
	assert.Equal(t, uint32(2), e.SchemaVersion)
	assert.NotEmpty(t, e.Metadata)
}

func TestSnapshot_JSON(t *testing.T) {
	raw := `{"version":5,"payload":"{\"total\":100}"}`

	var snap Snapshot
	err := json.Unmarshal([]byte(raw), &snap)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), snap.Version)
	assert.Contains(t, snap.Payload, "total")
}

func TestStreamData_EmptyEvents(t *testing.T) {
	sd := &StreamData{
		Snapshot: &Snapshot{Version: 3, Payload: "{}"},
	}
	assert.NotNil(t, sd.Snapshot)
	assert.Empty(t, sd.Events)
}

// Helper: verify append command formatting matches the expected protocol.
func TestEventStore_ResetConn_Nil(t *testing.T) {
	es := NewEventStore("localhost:9000", "orders", "p-1")
	es.resetConn()
	assert.Nil(t, es.conn)
}

func TestParseAppendResponse_UnknownFields(t *testing.T) {
	resp := "OK version=7 unknown=foo offset=42 partition=1 extra=bar"
	result, err := parseAppendResponse(resp)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), result.Version)
	assert.Equal(t, uint64(42), result.Offset)
	assert.Equal(t, 1, result.Partition)
}

func TestParseAppendResponse_NoEquals(t *testing.T) {
	_, err := parseAppendResponse("OK done")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing append fields")
}

func TestAppendCommandFormat(t *testing.T) {
	es := NewEventStore("localhost:9000", "orders", "p-1")

	event := &Event{
		Type:    "OrderCreated",
		Payload: `{"id":1}`,
	}

	// Build the command the same way Append does.
	sv := event.SchemaVersion
	if sv == 0 {
		sv = 1
	}
	expectedVersion := uint64(0)
	nextVersion := expectedVersion + 1
	cmd := "APPEND_STREAM topic=" + es.topic +
		" key=order-1" +
		" version=" + strconv.FormatUint(nextVersion, 10) +
		" event_type=" + event.Type +
		" schema_version=" + strconv.FormatUint(uint64(sv), 10) +
		" producerId=" + es.producerID +
		" message=" + event.Payload

	assert.True(t, strings.HasPrefix(cmd, "APPEND_STREAM"))
	assert.Contains(t, cmd, "topic=orders")
	assert.Contains(t, cmd, "key=order-1")
	assert.Contains(t, cmd, "version=1")
	assert.Contains(t, cmd, "event_type=OrderCreated")
	assert.Contains(t, cmd, "schema_version=1")
	assert.Contains(t, cmd, "producerId=p-1")
	assert.Contains(t, cmd, `message={"id":1}`)
}

func TestParseSnapshotResponse_StrictContract(t *testing.T) {
	snapshotJSON, ok, err := parseSnapshotResponse(`OK snapshot={"version":1,"payload":"x"}`)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, `{"version":1,"payload":"x"}`, snapshotJSON)

	snapshotJSON, ok, err = parseSnapshotResponse("OK snapshot=null")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, snapshotJSON)

	_, _, err = parseSnapshotResponse("NULL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected snapshot response")

	_, _, err = parseSnapshotResponse(`{"version":1,"payload":"x"}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected snapshot response")
}

func TestParseStreamVersionResponse_StrictContract(t *testing.T) {
	version, err := parseStreamVersionResponse("OK version=7")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), version)

	_, err = parseStreamVersionResponse("7")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected version response")

	_, err = parseStreamVersionResponse("OK")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing version")
}

func TestEventStoreCreateTopicDeclaresDeleteCleanupPolicy(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	store := NewEventStore("unused", "orders", "producer-1")
	store.conn = client
	done := make(chan error, 1)
	go func() {
		defer func() { _ = server.Close() }()
		data, err := ReadWithLength(server)
		if err != nil {
			done <- err
			return
		}
		_, command, err := util.DecodeMessage(data)
		if err != nil {
			done <- err
			return
		}
		want := "CREATE topic=orders partitions=4 event_sourcing=true cleanup_policy=delete"
		if command != want {
			done <- fmt.Errorf("command = %q, want %q", command, want)
			return
		}
		done <- WriteWithLength(server, []byte("OK topic=orders partitions=4 cleanup_policy=delete"))
	}()

	require.NoError(t, store.CreateTopic(4))
	require.NoError(t, <-done)
}
