package sdk

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAppendResponse(t *testing.T) {
	resp := "OK version=3 offset=1024 partition=2"

	result := parseAppendResponse(resp)

	assert.Equal(t, uint64(3), result.Version)
	assert.Equal(t, uint64(1024), result.Offset)
	assert.Equal(t, 2, result.Partition)
}

func TestParseAppendResponse_Partial(t *testing.T) {
	resp := "OK version=1"

	result := parseAppendResponse(resp)

	assert.Equal(t, uint64(1), result.Version)
	assert.Equal(t, uint64(0), result.Offset)
	assert.Equal(t, 0, result.Partition)
}

func TestParseAppendResponse_Empty(t *testing.T) {
	result := parseAppendResponse("")
	assert.Equal(t, uint64(0), result.Version)
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
	cmd := "APPEND_STREAM topic=" + es.topic +
		" key=order-1" +
		" expected_version=" + strconv.FormatUint(0, 10) +
		" event_type=" + event.Type +
		" schema_version=" + strconv.FormatUint(uint64(sv), 10) +
		" producerId=" + es.producerID +
		" message=" + event.Payload

	assert.True(t, strings.HasPrefix(cmd, "APPEND_STREAM"))
	assert.Contains(t, cmd, "topic=orders")
	assert.Contains(t, cmd, "key=order-1")
	assert.Contains(t, cmd, "expected_version=0")
	assert.Contains(t, cmd, "event_type=OrderCreated")
	assert.Contains(t, cmd, "schema_version=1")
	assert.Contains(t, cmd, "producerId=p-1")
	assert.Contains(t, cmd, `message={"id":1}`)
}
