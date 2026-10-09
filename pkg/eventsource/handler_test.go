package eventsource

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test helpers ---

type fakeStorageHandler struct {
	mu         sync.Mutex
	msgs       []types.Message
	offset     uint64
	first      uint64
	readErr    error
	beforeRead func(uint64)
}

func (f *fakeStorageHandler) ReadMessages(off uint64, max int) ([]types.Message, error) {
	if f.beforeRead != nil {
		f.beforeRead(off)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	var result []types.Message
	for _, m := range f.msgs {
		if m.Offset >= off && len(result) < max {
			result = append(result, m)
		}
	}
	return result, nil
}

func (f *fakeStorageHandler) GetAbsoluteOffset() uint64      { return f.offset }
func (f *fakeStorageHandler) GetFirstOffset() uint64         { return f.first }
func (f *fakeStorageHandler) GetFlushedOffset() uint64       { return f.offset }
func (f *fakeStorageHandler) GetLatestOffset() uint64        { return f.offset }
func (f *fakeStorageHandler) GetSegmentPath(_ uint64) string { return "" }

func (f *fakeStorageHandler) AppendMessage(_ string, _ int, msg *types.Message) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := f.offset
	msg.Offset = off
	f.msgs = append(f.msgs, *msg)
	f.offset++
	return off, nil
}

func (f *fakeStorageHandler) AppendMessageSync(t string, p int, msg *types.Message) (uint64, error) {
	return f.AppendMessage(t, p, msg)
}

func (f *fakeStorageHandler) AppendMessageWithOffset(t string, p int, msg *types.Message) error {
	return nil
}

func (f *fakeStorageHandler) WriteBatch(batch []types.DiskMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, diskMsg := range batch {
		f.msgs = append(f.msgs, types.Message{
			Offset:                       diskMsg.Offset,
			ProducerID:                   diskMsg.ProducerID,
			SeqNum:                       diskMsg.SeqNum,
			Epoch:                        diskMsg.Epoch,
			Payload:                      diskMsg.Payload,
			Key:                          diskMsg.Key,
			EventType:                    diskMsg.EventType,
			SchemaVersion:                diskMsg.SchemaVersion,
			AggregateVersion:             diskMsg.AggregateVersion,
			Metadata:                     diskMsg.Metadata,
			TransactionalID:              diskMsg.TransactionalID,
			TransactionState:             diskMsg.TransactionState,
			TransactionMarker:            diskMsg.TransactionMarker,
			ControlBatchType:             diskMsg.ControlBatchType,
			ControlBatchVersion:          diskMsg.ControlBatchVersion,
			ControlBatchCoordinatorEpoch: diskMsg.ControlBatchCoordinatorEpoch,
			ControlBatchKey:              append([]byte(nil), diskMsg.ControlBatchKey...),
			ControlBatchValue:            append([]byte(nil), diskMsg.ControlBatchValue...),
		})
		if next := diskMsg.Offset + 1; next > f.offset {
			f.offset = next
		}
	}
	return nil
}
func (f *fakeStorageHandler) TruncateTo(_ uint64) error { return nil }
func (f *fakeStorageHandler) Flush()                    {}
func (f *fakeStorageHandler) Close() error              { return nil }

type fakeHandlerProvider struct {
	handlers map[string]*fakeStorageHandler
}

func newFakeHandlerProvider() *fakeHandlerProvider {
	return &fakeHandlerProvider{handlers: make(map[string]*fakeStorageHandler)}
}

func (fp *fakeHandlerProvider) GetHandler(topicName string, partitionID int) (types.StorageHandler, error) {
	key := fmt.Sprintf("%s:%d", topicName, partitionID)
	h, ok := fp.handlers[key]
	if !ok {
		h = &fakeStorageHandler{}
		fp.handlers[key] = h
	}
	return h, nil
}

type fakeStreamManager struct{}

func (f *fakeStreamManager) AddStream(_ string, _ *stream.StreamConnection, _ func(uint64, int) ([]types.Message, error)) error {
	return nil
}
func (f *fakeStreamManager) RemoveStream(_ string) {}
func (f *fakeStreamManager) GetStreamsForPartition(_ string, _ int) []*stream.StreamConnection {
	return nil
}
func (f *fakeStreamManager) StopStream(_ string) {}

// newTestHandler creates a Handler backed by a TopicManager with a single event-sourcing topic.
func newTestHandler(t *testing.T) *Handler {
	h, _ := newTestHandlerWithProvider(t)
	return h
}

func newTestHandlerWithProvider(t *testing.T) (*Handler, *fakeHandlerProvider) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{LogDir: dir}
	hp := newFakeHandlerProvider()
	sm := &fakeStreamManager{}
	tm := topic.NewTopicManager(cfg, hp, sm)
	err := tm.CreateTopic("orders", 1, false, true)
	require.NoError(t, err)
	// Also create a non-event-sourcing topic for negative tests.
	err = tm.CreateTopic("plain", 1, false, false)
	require.NoError(t, err)
	return NewHandler(tm), hp
}

// TestAppendAndReadStream_Integration verifies that appending events updates
// the version correctly and that snapshot-based lookup skips already-snapshotted versions.
func TestAppendAndReadStream_Integration(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	ss, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()

	key := "order-123"

	// Append 3 events for the same aggregate key.
	for v := uint64(1); v <= 3; v++ {
		err := idx.Append(key, v, v*100, 0)
		require.NoError(t, err, "append version %d should succeed", v)
	}

	// Verify the current version is 3.
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Save a snapshot at version 2.
	err = ss.Save(key, 2, `{"total":200}`)
	require.NoError(t, err)

	snap, err := ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(2), snap.Version)
	assert.Equal(t, `{"total":200}`, snap.Payload)

	// Lookup from snapshot.Version+1 should return only version 3.
	entries, err := idx.Lookup(key, snap.Version+1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(3), entries[0].AggregateVersion)
	assert.Equal(t, uint64(300), entries[0].Offset)
}

// TestVersionConflict verifies that the version counter advances correctly
// after each append, which is used by the handler to detect optimistic concurrency conflicts.
func TestVersionConflict(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "account-456"

	// Append 2 events.
	require.NoError(t, idx.Append(key, 1, 10, 0))
	require.NoError(t, idx.Append(key, 2, 20, 0))

	// Verify current version is 2.
	assert.Equal(t, uint64(2), idx.GetVersion(key))

	// Simulate a second writer that successfully appends version 3.
	require.NoError(t, idx.Append(key, 3, 30, 0))

	// Now GetVersion returns 3, so the original writer with expected_version=2
	// would detect a conflict (expected_version != currentVersion+1).
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Verify all 3 entries are present.
	entries, err := idx.Lookup(key, 1)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
}

// TestSnapshotVersionValidation verifies that snapshot versions are validated
// against the current stream version: a snapshot version exceeding the stream
// version is invalid, while saving at the current version succeeds. The store
// also rejects regressions so direct callers cannot bypass handler validation.
func TestSnapshotVersionValidation(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	ss, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()

	key := "cart-789"

	// Append 3 events to reach version 3.
	for v := uint64(1); v <= 3; v++ {
		require.NoError(t, idx.Append(key, v, v*10, 0))
	}
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Simulate the handler's version validation:
	// snapshot version > currentVersion should be rejected.
	currentVersion := idx.GetVersion(key)
	snapshotVersion := uint64(5)
	assert.True(t, snapshotVersion > currentVersion,
		"snapshot version %d should exceed stream version %d — handler would reject this", snapshotVersion, currentVersion)

	// Version at or below current should pass validation.
	assert.False(t, uint64(3) > currentVersion, "version 3 should pass validation")
	assert.False(t, uint64(1) > currentVersion, "version 1 should pass validation")

	// Save at version 3 (current) should succeed.
	err = ss.Save(key, 3, `{"items":["a","b","c"]}`)
	require.NoError(t, err)

	snap, err := ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(3), snap.Version)
	assert.Equal(t, `{"items":["a","b","c"]}`, snap.Payload)

	// A lower version must not replace the current durable snapshot.
	err = ss.Save(key, 2, `{"items":["a","b"]}`)
	require.ErrorContains(t, err, "snapshot version regression")

	snap, err = ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(3), snap.Version)
	assert.Equal(t, `{"items":["a","b","c"]}`, snap.Payload)
}

// TestMultipleAggregates verifies that events appended to different aggregate keys
// maintain independent version tracking.
func TestMultipleAggregates(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	keys := []string{"order-1", "order-2", "order-3"}

	// Append a different number of events per key.
	for i, key := range keys {
		count := uint64(i + 1) // 1, 2, 3 events respectively
		for v := uint64(1); v <= count; v++ {
			err := idx.Append(key, v, uint64(i*100)+v, 0)
			require.NoError(t, err)
		}
	}

	// Verify each key has its own independent version.
	assert.Equal(t, uint64(1), idx.GetVersion("order-1"))
	assert.Equal(t, uint64(2), idx.GetVersion("order-2"))
	assert.Equal(t, uint64(3), idx.GetVersion("order-3"))

	// Verify lookup returns only entries for the requested key.
	entries1, err := idx.Lookup("order-1", 1)
	require.NoError(t, err)
	assert.Len(t, entries1, 1)

	entries2, err := idx.Lookup("order-2", 1)
	require.NoError(t, err)
	assert.Len(t, entries2, 2)

	entries3, err := idx.Lookup("order-3", 1)
	require.NoError(t, err)
	assert.Len(t, entries3, 3)

	// A key that was never appended should have version 0.
	assert.Equal(t, uint64(0), idx.GetVersion("nonexistent"))
}

// --- Handler method tests ---

func TestHandler_HandleAppendStream_Validation(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	tests := []struct {
		name string
		cmd  string
		want string
	}{
		{"missing topic", "APPEND_STREAM key=k1 version=1 message=hi", "ERROR: missing_topic"},
		{"missing key", "APPEND_STREAM topic=orders version=1 message=hi", "ERROR: missing_key"},
		{"missing version", "APPEND_STREAM topic=orders key=k1 message=hi", "ERROR: missing_version"},
		{"invalid version", "APPEND_STREAM topic=orders key=k1 version=abc message=hi", "ERROR: invalid_version"},
		{"missing message", "APPEND_STREAM topic=orders key=k1 version=1", "ERROR: missing_message"},
		{"nonexistent topic", "APPEND_STREAM topic=nope key=k1 version=1 message=hi", "ERROR: topic_not_found topic=nope"},
		{"non-eventsourcing topic", "APPEND_STREAM topic=plain key=k1 version=1 message=hi", "ERROR: event_sourcing_not_enabled topic=plain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.HandleAppendStream(tt.cmd)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestHandler_HandleAppendStream_Success(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	result := h.HandleAppendStream("APPEND_STREAM topic=orders key=order-1 version=1 message={\"item\":\"A\"}")
	assert.Contains(t, result, "OK version=1")
	assert.Contains(t, result, "offset=0")
	assert.Contains(t, result, "partition=0")

	// Second append should succeed at version 2.
	result = h.HandleAppendStream("APPEND_STREAM topic=orders key=order-1 version=2 message={\"item\":\"B\"}")
	assert.Contains(t, result, "OK version=2")
}

func TestHandler_HandleAppendStream_VersionConflict(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	// Seed version 1.
	result := h.HandleAppendStream("APPEND_STREAM topic=orders key=order-1 version=1 message=init")
	assert.Contains(t, result, "OK")

	// Attempting version 1 again should conflict.
	result = h.HandleAppendStream("APPEND_STREAM topic=orders key=order-1 version=1 message=dup")
	assert.Contains(t, result, "ERROR: version_conflict")
	assert.Contains(t, result, "current=1")
	assert.Contains(t, result, "expected=1")
}

func TestHandler_HandleAppendStream_RejectsReusedAggregateEventID(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()
	require.NoError(t, h.tm.CreateTopicWithPolicy("matches", 1, true, true, topic.Policy{AggregateReplay: true}))

	first := "APPEND_STREAM topic=matches key=match-1 version=1 event_id=event-1 producerId=producer-1 seqNum=1 message=started"
	second := "APPEND_STREAM topic=matches key=match-1 version=2 event_id=event-1 producerId=producer-1 seqNum=2 message=finished"
	require.True(t, strings.HasPrefix(h.HandleAppendStream(first), "OK "))
	require.Contains(t, h.HandleAppendStream(second), "aggregate_identity_conflict")
}

func TestHandler_HandleStreamVersion(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	// Version of nonexistent key is 0.
	result := h.HandleStreamVersion("STREAM_VERSION topic=orders key=new-key")
	assert.Equal(t, "OK version=0", result)

	// Append and check version.
	h.HandleAppendStream("APPEND_STREAM topic=orders key=sv-key version=1 message=data")
	result = h.HandleStreamVersion("STREAM_VERSION topic=orders key=sv-key")
	assert.Equal(t, "OK version=1", result)
}

func TestHandler_HandleStreamVersion_Validation(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	tests := []struct {
		name string
		cmd  string
		want string
	}{
		{"missing topic", "STREAM_VERSION key=k1", "ERROR: missing_topic"},
		{"missing key", "STREAM_VERSION topic=orders", "ERROR: missing_key"},
		{"nonexistent topic", "STREAM_VERSION topic=nope key=k1", "ERROR: topic_not_found topic=nope"},
		{"non-eventsourcing", "STREAM_VERSION topic=plain key=k1", "ERROR: event_sourcing_not_enabled topic=plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.HandleStreamVersion(tt.cmd)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestHandler_HandleReadStreamRejectsInvalidFromVersion(t *testing.T) {
	h := NewHandler(nil)
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = server.Close() }()
		h.HandleReadStream("READ_STREAM topic=orders key=order-1 from_version=invalid", server)
	}()

	payload, err := util.ReadWithLength(client)
	require.NoError(t, err)
	require.Equal(t, "ERROR: invalid_from_version", string(payload))
	<-done
}
func TestHandler_HandleSaveSnapshot_Validation(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	tests := []struct {
		name string
		cmd  string
		want string
	}{
		{"missing topic", "SAVE_SNAPSHOT key=k1 version=1 message=data", "ERROR: missing_topic"},
		{"missing key", "SAVE_SNAPSHOT topic=orders version=1 message=data", "ERROR: missing_key"},
		{"missing version", "SAVE_SNAPSHOT topic=orders key=k1 message=data", "ERROR: missing_version"},
		{"invalid version", "SAVE_SNAPSHOT topic=orders key=k1 version=xyz message=data", "ERROR: invalid_version"},
		{"missing message", "SAVE_SNAPSHOT topic=orders key=k1 version=1", "ERROR: missing_message"},
		{"nonexistent topic", "SAVE_SNAPSHOT topic=nope key=k1 version=1 message=data", "ERROR: topic_not_found topic=nope"},
		{"non-eventsourcing", "SAVE_SNAPSHOT topic=plain key=k1 version=1 message=data", "ERROR: event_sourcing_not_enabled topic=plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.HandleSaveSnapshot(tt.cmd)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestHandler_HandleSaveSnapshot_VersionExceedsStream(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	// Stream is at version 0 (no events). Saving snapshot at version 5 should fail.
	result := h.HandleSaveSnapshot("SAVE_SNAPSHOT topic=orders key=k1 version=5 message={}")
	assert.Contains(t, result, "ERROR: snapshot_version_exceeds_stream version=5 current=0")
}

func TestHandler_HandleSaveAndReadSnapshot(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	// Append an event so the stream reaches version 1.
	result := h.HandleAppendStream("APPEND_STREAM topic=orders key=snap-key version=1 message=event1")
	assert.Contains(t, result, "OK")

	// Save a snapshot at version 1.
	result = h.HandleSaveSnapshot("SAVE_SNAPSHOT topic=orders key=snap-key version=1 message={\"total\":100}")
	assert.Contains(t, result, "OK version=1")

	// Read it back.
	result = h.HandleReadSnapshot("READ_SNAPSHOT topic=orders key=snap-key")
	assert.Contains(t, result, `"version":1`)
	assert.Contains(t, result, `"payload":"{\"total\":100}"`)
}

func TestHandler_HandleReadSnapshot_Validation(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	tests := []struct {
		name string
		cmd  string
		want string
	}{
		{"missing topic", "READ_SNAPSHOT key=k1", "ERROR: missing_topic"},
		{"missing key", "READ_SNAPSHOT topic=orders", "ERROR: missing_key"},
		{"nonexistent topic", "READ_SNAPSHOT topic=nope key=k1", "ERROR: topic_not_found topic=nope"},
		{"non-eventsourcing", "READ_SNAPSHOT topic=plain key=k1", "ERROR: event_sourcing_not_enabled topic=plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.HandleReadSnapshot(tt.cmd)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestHandler_HandleReadSnapshot_NotFound(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	result := h.HandleReadSnapshot("READ_SNAPSHOT topic=orders key=nope")
	assert.Equal(t, "OK snapshot=null", result)
}

func TestHandler_NewHandlerAndClose(t *testing.T) {
	h := newTestHandler(t)

	// Trigger lazy creation of an index and snapshot store.
	h.HandleAppendStream("APPEND_STREAM topic=orders key=close-key version=1 message=data")
	h.HandleReadSnapshot("READ_SNAPSHOT topic=orders key=close-key")

	// Close should succeed.
	err := h.Close()
	assert.NoError(t, err)
}

func TestParseKeyValueArgs(t *testing.T) {
	t.Run("standard_key_value_pairs", func(t *testing.T) {
		result := parseKeyValueArgs("topic=orders key=order-1 version=3")
		assert.Equal(t, "orders", result["topic"])
		assert.Equal(t, "order-1", result["key"])
		assert.Equal(t, "3", result["version"])
	})

	t.Run("message_preserves_spaces", func(t *testing.T) {
		result := parseKeyValueArgs(`topic=orders key=k1 message={"name": "hello world"}`)
		assert.Equal(t, "orders", result["topic"])
		assert.Equal(t, "k1", result["key"])
		assert.Equal(t, `{"name": "hello world"}`, result["message"])
	})

	t.Run("empty_value", func(t *testing.T) {
		result := parseKeyValueArgs("topic= key=abc")
		assert.Equal(t, "", result["topic"])
		assert.Equal(t, "abc", result["key"])
	})

	t.Run("message_at_start", func(t *testing.T) {
		result := parseKeyValueArgs("message=hello world this is payload")
		assert.Equal(t, "hello world this is payload", result["message"])
		// No other keys should be present.
		assert.Empty(t, result["topic"])
	})

	t.Run("no_message", func(t *testing.T) {
		result := parseKeyValueArgs("topic=events key=user-5 version=1")
		assert.Equal(t, "events", result["topic"])
		assert.Equal(t, "user-5", result["key"])
		assert.Equal(t, "1", result["version"])
		_, hasMessage := result["message"]
		assert.False(t, hasMessage, "message key should not exist when not provided")
	})

	t.Run("empty_string", func(t *testing.T) {
		result := parseKeyValueArgs("")
		assert.Empty(t, result)
	})

	t.Run("message_with_equals_signs", func(t *testing.T) {
		result := parseKeyValueArgs(`topic=t1 message=base64data==more=stuff`)
		assert.Equal(t, "t1", result["topic"])
		assert.Equal(t, "base64data==more=stuff", result["message"])
	})

	t.Run("keys_after_message_are_consumed_as_payload", func(t *testing.T) {
		result := parseKeyValueArgs("topic=t1 message=payload key=should-be-in-message")
		assert.Equal(t, "t1", result["topic"])
		assert.Equal(t, "payload key=should-be-in-message", result["message"])
		// key should NOT be separately parsed since it comes after message=
		assert.NotEqual(t, "should-be-in-message", result["key"])
	})
}

func TestHandler_IndexesReplicatedMessagesOnlyAfterHWMCommit(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	require.NoError(t, h.PrepareCommittedIndex("orders", 0))
	topic := h.tm.GetTopic("orders")
	require.NotNil(t, topic)
	p, err := topic.GetPartition(0)
	require.NoError(t, err)

	messages := []types.Message{{
		Key:              "replicated-key",
		Payload:          "event1",
		EventType:        "Replicated",
		SchemaVersion:    1,
		AggregateVersion: 1,
	}}
	require.NoError(t, p.EnqueueBatchLeader(messages))
	p.FlushDisk()

	assert.Equal(t, "OK version=0", h.HandleStreamVersion("STREAM_VERSION topic=orders key=replicated-key"))

	require.NoError(t, p.ApplyReplicaHWM(1))
	require.NoError(t, h.IndexCommittedToHWM("orders", 0, 1))
	assert.Equal(t, "OK version=1", h.HandleStreamVersion("STREAM_VERSION topic=orders key=replicated-key"))
}

func TestHandler_RecoverIndexDiscardsEntriesBeyondCommittedHWM(t *testing.T) {
	h := newTestHandler(t)
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, idx.Append("uncommitted-key", 1, 0, 0))
	require.NoError(t, h.Close())

	h2 := NewHandler(h.tm)
	defer func() { _ = h2.Close() }()
	assert.Equal(t, "OK version=0", h2.HandleStreamVersion("STREAM_VERSION topic=orders key=uncommitted-key"))
}
func TestHandler_HandleAppendStream_DefaultSchemaVersion(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	result := h.HandleAppendStream("APPEND_STREAM topic=orders key=schema-key version=1 message=event1")
	require.Contains(t, result, "OK version=1")

	topic := h.tm.GetTopic("orders")
	require.NotNil(t, topic)
	p, err := topic.GetPartition(0)
	require.NoError(t, err)
	msgs, err := p.ReadCommitted(0, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, uint32(1), msgs[0].SchemaVersion)
}
func TestHandler_RecoverIndexFromCommittedLog(t *testing.T) {
	h := newTestHandler(t)

	result := h.HandleAppendStream("APPEND_STREAM topic=orders key=recover-key version=1 message=event1")
	assert.Contains(t, result, "OK version=1")

	dir := h.tm.GetLogDir("orders", 0)
	assert.NoError(t, h.Close())
	assert.NoError(t, os.Remove(filepath.Join(dir, "partition_0_stream.idx")))
	assert.NoError(t, os.Remove(filepath.Join(dir, "partition_0_stream_keys.dat")))

	h2 := NewHandler(h.tm)
	defer func() { _ = h2.Close() }()

	result = h2.HandleStreamVersion("STREAM_VERSION topic=orders key=recover-key")
	assert.Equal(t, "OK version=1", result)
}

func TestHandler_RecoverIndexPastTransactionMarkerTailAfterRestart(t *testing.T) {
	h := newTestHandler(t)
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=recover-key version=1 message=event1"), "OK version=1")

	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{
		Topic: "orders", TransactionalID: "completed-transaction", TransactionMarker: types.TransactionMarkerCommit,
	}}))
	p.FlushDisk()
	require.NoError(t, p.ApplyReplicaHWM(2))
	require.Equal(t, uint64(2), p.LastStableOffset())

	dir := h.tm.GetLogDir("orders", 0)
	require.NoError(t, h.Close())
	require.NoError(t, os.Remove(filepath.Join(dir, "partition_0_stream.idx")))
	require.NoError(t, os.Remove(filepath.Join(dir, "partition_0_stream_keys.dat")))

	h2 := NewHandler(h.tm)
	defer func() { _ = h2.Close() }()
	require.Equal(t, "OK version=1", h2.HandleStreamVersion("STREAM_VERSION topic=orders key=recover-key"))
}

func TestHandler_RecoverIndexSkipsTransactionRecordsAcrossPages(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()

	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	messages := make([]types.Message, 0, 523)
	for i := 0; i < 260; i++ {
		messages = append(messages,
			types.Message{Topic: "orders", Key: fmt.Sprintf("visible-%03d", i), AggregateVersion: 1},
			types.Message{Topic: "orders", TransactionalID: fmt.Sprintf("committed-%03d", i), TransactionMarker: types.TransactionMarkerCommit},
		)
	}
	messages = append(messages,
		types.Message{Topic: "orders", Key: "aborted-key", AggregateVersion: 1, TransactionalID: "aborted", TransactionState: types.TransactionStateAborted},
		types.Message{Topic: "orders", TransactionalID: "aborted", TransactionMarker: types.TransactionMarkerAbort},
		types.Message{Topic: "orders", Key: "final-visible", AggregateVersion: 1},
	)
	require.NoError(t, p.EnqueueBatchLeader(messages))
	p.FlushDisk()
	require.NoError(t, p.ApplyReplicaHWM(uint64(len(messages))))

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, h.RecoverIndexFromLog("orders", 0, idx))
	require.Equal(t, uint64(1), idx.GetVersion("visible-000"))
	require.Equal(t, uint64(1), idx.GetVersion("visible-259"))
	require.Equal(t, uint64(1), idx.GetVersion("final-visible"))
	require.Zero(t, idx.GetVersion("aborted-key"))
}

func TestHandler_RecoverIndexUsesCapturedStableTail(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=stable-key version=1 message=stable"), "OK version=1")

	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	storage := hp.handlers["orders:0"]
	var appendOnce sync.Once
	storage.beforeRead = func(uint64) {
		appendOnce.Do(func() {
			require.NoError(t, p.EnqueueBatchLeader([]types.Message{{
				Topic: "orders", Key: "late-key", Payload: "late", AggregateVersion: 1,
			}}))
			p.FlushDisk()
			require.NoError(t, p.ApplyReplicaHWM(2))
		})
	}

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, h.RecoverIndexFromLog("orders", 0, idx))
	require.Equal(t, uint64(1), idx.GetVersion("stable-key"))
	require.Zero(t, idx.GetVersion("late-key"))
}

func TestHandler_RecoverIndexFromRetentionCheckpoint(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=retained-key version=1 message=event1"), "OK version=1")
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=retained-key version=2 message=event2"), "OK version=2")

	ss, err := NewSnapshotStore(h.tm.GetLogDir("orders", 0), 0)
	require.NoError(t, err)
	require.NoError(t, ss.Save("retained-key", 1, "snapshot"))
	require.NoError(t, ss.Close())
	hp.handlers["orders:0"].first = 1

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, h.RecoverIndexFromLog("orders", 0, idx))
	require.Equal(t, uint64(2), idx.GetVersion("retained-key"))
}

func TestHandler_RecoverIndexRejectsRetentionWithoutCheckpoint(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=retained-key version=1 message=event1"), "OK version=1")
	hp.handlers["orders:0"].first = 1

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	err = h.RecoverIndexFromLog("orders", 0, idx)
	require.ErrorContains(t, err, "cannot recover retained event stream at offset 1 without a snapshot checkpoint")
}

func TestHandler_RecoverIndexKeepsDurableReadFailuresFailClosed(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=unreadable-key version=1 message=event1"), "OK version=1")
	hp.handlers["orders:0"].readErr = fmt.Errorf("unreadable durable record")

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	err = h.RecoverIndexFromLog("orders", 0, idx)
	require.ErrorContains(t, err, "recover stream index from log offset=0")
	require.ErrorContains(t, err, "unreadable durable record")
}

func TestHandler_RecoverIndexRejectsGapBeforeStableTail(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=gap-key version=1 message=event1"), "OK version=1")

	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	storage := hp.handlers["orders:0"]
	storage.mu.Lock()
	storage.offset = 2
	storage.mu.Unlock()
	p.SetHWM(2)

	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	err = h.RecoverIndexFromLog("orders", 0, idx)
	require.ErrorContains(t, err, "recover stream index stopped before stable tail at offset=1 tail=2")
}
