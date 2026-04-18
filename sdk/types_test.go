package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Message.String()
// ---------------------------------------------------------------------------

func TestMessage_String_Basic(t *testing.T) {
	m := Message{
		ProducerID: "prod-1",
		SeqNum:     42,
		Payload:    "hello",
		Offset:     100,
		Key:        "order-1",
		Epoch:      1700000000,
		RetryCount: 3,
	}
	s := m.String()
	assert.Contains(t, s, "prod-1-42")
	assert.Contains(t, s, "Payload:hello")
	assert.Contains(t, s, "Offset:100")
	assert.Contains(t, s, "Key:order-1")
	assert.Contains(t, s, "Epoch:1700000000")
	assert.Contains(t, s, "RetryCount:3")
}

func TestMessage_String_EmptyFields(t *testing.T) {
	m := Message{}
	s := m.String()
	assert.Contains(t, s, "-0") // empty ProducerID + SeqNum 0
	assert.Contains(t, s, "Payload:")
	assert.Contains(t, s, "Offset:0")
}

func TestMessage_String_Format(t *testing.T) {
	m := Message{ProducerID: "abc", SeqNum: 1}
	s := m.String()
	assert.Contains(t, s, "abc-1")
	assert.Contains(t, s, "Payload:")
	assert.Contains(t, s, "Offset:0")
}

// ---------------------------------------------------------------------------
// Message struct field defaults
// ---------------------------------------------------------------------------

func TestMessage_ZeroValue(t *testing.T) {
	var m Message
	assert.Equal(t, uint64(0), m.Offset)
	assert.Equal(t, "", m.ProducerID)
	assert.Equal(t, uint64(0), m.SeqNum)
	assert.Equal(t, "", m.Payload)
	assert.Equal(t, "", m.Key)
	assert.Equal(t, int64(0), m.Epoch)
	assert.Equal(t, "", m.EventType)
	assert.Equal(t, uint32(0), m.SchemaVersion)
	assert.Equal(t, uint64(0), m.AggregateVersion)
	assert.Equal(t, "", m.Metadata)
	assert.Equal(t, 0, m.RetryCount)
	assert.False(t, m.Retry)
}

func TestMessage_WithRetry(t *testing.T) {
	m := Message{
		Retry:      true,
		RetryCount: 5,
	}
	assert.True(t, m.Retry)
	assert.Equal(t, 5, m.RetryCount)
}

// ---------------------------------------------------------------------------
// AckResponse
// ---------------------------------------------------------------------------

func TestAckResponse_ZeroValue(t *testing.T) {
	var a AckResponse
	assert.Equal(t, "", a.Status)
	assert.Equal(t, uint64(0), a.LastOffset)
	assert.Equal(t, int64(0), a.ProducerEpoch)
	assert.Equal(t, "", a.ProducerID)
	assert.Equal(t, uint64(0), a.SeqStart)
	assert.Equal(t, uint64(0), a.SeqEnd)
	assert.Equal(t, "", a.Leader)
	assert.Equal(t, "", a.ErrorMsg)
}

func TestAckResponse_FieldValues(t *testing.T) {
	a := AckResponse{
		Status:        "ok",
		LastOffset:    1024,
		ProducerEpoch: 5,
		ProducerID:    "p-1",
		SeqStart:      1,
		SeqEnd:        10,
		Leader:        "broker-0",
		ErrorMsg:      "",
	}
	assert.Equal(t, "ok", a.Status)
	assert.Equal(t, uint64(1024), a.LastOffset)
	assert.Equal(t, int64(5), a.ProducerEpoch)
	assert.Equal(t, "p-1", a.ProducerID)
	assert.Equal(t, uint64(1), a.SeqStart)
	assert.Equal(t, uint64(10), a.SeqEnd)
	assert.Equal(t, "broker-0", a.Leader)
	assert.Empty(t, a.ErrorMsg)
}

// ---------------------------------------------------------------------------
// PartitionStat
// ---------------------------------------------------------------------------

func TestPartitionStat_ZeroValue(t *testing.T) {
	var ps PartitionStat
	assert.Equal(t, 0, ps.PartitionID)
	assert.Equal(t, 0, ps.BatchCount)
	assert.Equal(t, time.Duration(0), ps.AvgDuration)
}

func TestPartitionStat_FieldValues(t *testing.T) {
	ps := PartitionStat{
		PartitionID: 4,
		BatchCount:  100,
		AvgDuration: 5 * time.Millisecond,
	}
	assert.Equal(t, 4, ps.PartitionID)
	assert.Equal(t, 100, ps.BatchCount)
	assert.Equal(t, 5*time.Millisecond, ps.AvgDuration)
}

// ---------------------------------------------------------------------------
// Event-sourcing types (from eventstore.go, tested here for completeness)
// ---------------------------------------------------------------------------

func TestEvent_ZeroValue(t *testing.T) {
	var e Event
	assert.Equal(t, "", e.Type)
	assert.Equal(t, uint32(0), e.SchemaVersion)
	assert.Equal(t, "", e.Payload)
	assert.Equal(t, "", e.Metadata)
}

func TestStreamEvent_Fields(t *testing.T) {
	se := StreamEvent{
		Version:       3,
		Offset:        500,
		Type:          "OrderCreated",
		SchemaVersion: 1,
		Payload:       `{"id":1}`,
		Metadata:      `{"user":"u1"}`,
	}
	assert.Equal(t, uint64(3), se.Version)
	assert.Equal(t, uint64(500), se.Offset)
	assert.Equal(t, "OrderCreated", se.Type)
	assert.Equal(t, uint32(1), se.SchemaVersion)
	assert.Equal(t, `{"id":1}`, se.Payload)
	assert.Equal(t, `{"user":"u1"}`, se.Metadata)
}

func TestAppendResult_Fields(t *testing.T) {
	ar := AppendResult{
		Version:   10,
		Offset:    2048,
		Partition: 3,
	}
	assert.Equal(t, uint64(10), ar.Version)
	assert.Equal(t, uint64(2048), ar.Offset)
	assert.Equal(t, 3, ar.Partition)
}

func TestStreamData_WithEvents(t *testing.T) {
	sd := StreamData{
		Snapshot: &Snapshot{Version: 5, Payload: `{"balance":100}`},
		Events: []StreamEvent{
			{Version: 6, Payload: `{"amount":10}`},
			{Version: 7, Payload: `{"amount":20}`},
		},
	}
	assert.NotNil(t, sd.Snapshot)
	assert.Equal(t, uint64(5), sd.Snapshot.Version)
	assert.Len(t, sd.Events, 2)
	assert.Equal(t, uint64(6), sd.Events[0].Version)
	assert.Equal(t, uint64(7), sd.Events[1].Version)
}

func TestStreamData_NilSnapshot(t *testing.T) {
	sd := StreamData{}
	assert.Nil(t, sd.Snapshot)
	assert.Empty(t, sd.Events)
}
