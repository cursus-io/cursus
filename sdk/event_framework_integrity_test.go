package sdk

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func envelopeStreamEvent(t *testing.T, event EventEnvelope, version uint64) StreamEvent {
	t.Helper()
	event.AggregateVersion = version
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	return StreamEvent{
		Version:       version,
		Type:          event.EventType,
		SchemaVersion: event.SchemaVersion,
		Payload:       string(payload),
	}
}

func TestDecodeEventEnvelopeRejectsStreamMetadataMismatch(t *testing.T) {
	event, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]string{"winner": "p1"})
	require.NoError(t, err)
	event.AggregateVersion = 1
	raw := envelopeStreamEvent(t, event, 1)
	raw.Type = "OtherEvent"
	_, err = decodeEventEnvelope(raw)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match stream type")

	raw = envelopeStreamEvent(t, event, 1)
	raw.SchemaVersion = 2
	_, err = decodeEventEnvelope(raw)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match stream schema version")

	raw = envelopeStreamEvent(t, event, 1)
	raw.Version = 2
	_, err = decodeEventEnvelope(raw)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match stream version")
}

func TestAggregateRepositoryRejectsForeignAggregateIDOnSave(t *testing.T) {
	store := &repositoryStore{}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate {
		return &repositoryAggregate{id: id}
	})
	require.NoError(t, err)
	aggregate := &repositoryAggregate{id: "game-1"}
	event, err := NewEventEnvelope("game", "other-game", "GameCreated", map[string]string{"status": "open"})
	require.NoError(t, err)

	err = repository.Save(aggregate, []EventEnvelope{event})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match aggregate")
	require.Empty(t, store.events)
}

type snapshotAggregate struct {
	id      string
	version uint64
	status  string
}

func (a *snapshotAggregate) ID() string      { return a.id }
func (a *snapshotAggregate) Type() string    { return "game" }
func (a *snapshotAggregate) Version() uint64 { return a.version }
func (a *snapshotAggregate) Apply(event EventEnvelope) error {
	a.version = event.AggregateVersion
	a.status = string(event.Payload)
	return nil
}
func (a *snapshotAggregate) RestoreSnapshot(payload string, version uint64) error {
	a.version = version
	a.status = payload
	return nil
}

type snapshotStreamStore struct {
	StreamStore
	stream *StreamData
}

func (s *snapshotStreamStore) ReadStream(string) (*StreamData, error) {
	return s.stream, nil
}

func TestAggregateRepositoryRestoresSnapshotBeforeReplay(t *testing.T) {
	event, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]string{"status": "finished"})
	require.NoError(t, err)
	store := &snapshotStreamStore{stream: &StreamData{
		Snapshot: &Snapshot{Version: 2, Payload: "{\"status\":\"waiting\"}"},
		Events:   []StreamEvent{envelopeStreamEvent(t, event, 3)},
	}}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate {
		return &snapshotAggregate{id: id}
	})
	require.NoError(t, err)

	aggregate, err := repository.Load("game-1")
	require.NoError(t, err)
	require.Equal(t, uint64(3), aggregate.Version())
	require.Equal(t, "{\"status\":\"finished\"}", aggregate.(*snapshotAggregate).status)
}

type applyFailureAggregate struct {
	id string
}

func (a *applyFailureAggregate) ID() string      { return a.id }
func (a *applyFailureAggregate) Type() string    { return "game" }
func (a *applyFailureAggregate) Version() uint64 { return 0 }
func (a *applyFailureAggregate) Apply(EventEnvelope) error {
	return errors.New("projection rejected event")
}

func TestAggregateRepositoryPropagatesApplyFailure(t *testing.T) {
	event, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]string{"status": "finished"})
	require.NoError(t, err)
	store := &repositoryStore{events: []StreamEvent{envelopeStreamEvent(t, event, 1)}}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate {
		return &applyFailureAggregate{id: id}
	})
	require.NoError(t, err)

	_, err = repository.Load("game-1")
	require.EqualError(t, err, "apply GameFinished v1: projection rejected event")
}

func TestReplaySkipsOlderVersionsAndStopsOnHandlerError(t *testing.T) {
	first, err := NewEventEnvelope("game", "game-1", "GameCreated", map[string]string{"status": "open"})
	require.NoError(t, err)
	second, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]string{"status": "finished"})
	require.NoError(t, err)
	store := &repositoryStore{events: []StreamEvent{
		envelopeStreamEvent(t, first, 1),
		envelopeStreamEvent(t, second, 2),
	}}

	var replayed []uint64
	err = Replay(store, "game-1", 2, nil, func(event EventEnvelope) error {
		replayed = append(replayed, event.AggregateVersion)
		return errors.New("projection unavailable")
	})
	require.EqualError(t, err, "replay GameFinished v2: projection unavailable")
	require.Equal(t, []uint64{2}, replayed)
}
