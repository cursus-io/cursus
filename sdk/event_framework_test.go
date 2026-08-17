package sdk

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEventEnvelopeAssignsIdentityAndUTC(t *testing.T) {
	event, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]any{"winner": "p1"})
	require.NoError(t, err)
	require.NotEmpty(t, event.EventID)
	require.Equal(t, "GameFinished", event.EventType)
	require.Equal(t, uint32(1), event.SchemaVersion)
	require.Equal(t, "game-1", event.AggregateID)
	require.False(t, event.OccurredAt.IsZero())
	require.Equal(t, "UTC", event.OccurredAt.Location().String())
}

func TestEventEnvelopeWireFormatKeepsPayloadAndMetadata(t *testing.T) {
	event, err := NewEventEnvelope("game", "game-1", "GameFinished", map[string]any{"winner": "p1"})
	require.NoError(t, err)
	event.CorrelationID = "corr-1"
	event.AggregateVersion = 1
	wire, err := event.wireEvent()
	require.NoError(t, err)
	var decoded EventEnvelope
	require.NoError(t, json.Unmarshal([]byte(wire.Payload), &decoded))
	require.Equal(t, event.EventID, decoded.EventID)
	require.Equal(t, event.CorrelationID, decoded.CorrelationID)
	require.JSONEq(t, `{"winner":"p1"}`, string(decoded.Payload))
}

type repositoryAggregate struct {
	id      string
	version uint64
	events  []string
}

func (a *repositoryAggregate) ID() string      { return a.id }
func (a *repositoryAggregate) Type() string    { return "game" }
func (a *repositoryAggregate) Version() uint64 { return a.version }
func (a *repositoryAggregate) Apply(event EventEnvelope) error {
	a.version = event.AggregateVersion
	a.events = append(a.events, event.EventType)
	return nil
}

type repositoryStore struct {
	events []StreamEvent
}

func (s *repositoryStore) ReadStream(string) (*StreamData, error) {
	return &StreamData{Events: append([]StreamEvent(nil), s.events...)}, nil
}

func (s *repositoryStore) Append(_ string, expected uint64, event *Event) (*AppendResult, error) {
	version := expected + 1
	s.events = append(s.events, StreamEvent{Version: version, Type: event.Type, SchemaVersion: event.SchemaVersion, Payload: event.Payload})
	return &AppendResult{Version: version}, nil
}

func TestAggregateRepositoryLoadsAndSavesWithExpectedVersion(t *testing.T) {
	store := &repositoryStore{}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate { return &repositoryAggregate{id: id} })
	require.NoError(t, err)
	aggregate := &repositoryAggregate{id: "game-1"}
	created, err := NewEventEnvelope("game", "game-1", "GameCreated", map[string]any{"status": "open"})
	require.NoError(t, err)
	require.NoError(t, repository.Save(aggregate, []EventEnvelope{created}))
	require.Equal(t, uint64(1), aggregate.Version())

	loaded, err := repository.Load("game-1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), loaded.Version())
	require.Equal(t, "GameCreated", loaded.(*repositoryAggregate).events[0])
}

func TestAggregateRepositoryRejectsMalformedStreamEnvelope(t *testing.T) {
	store := &repositoryStore{events: []StreamEvent{{Version: 1, Type: "GameCreated", SchemaVersion: 1, Payload: "not-json"}}}
	repository, err := NewAggregateRepository(store, func(id string) Aggregate { return &repositoryAggregate{id: id} })
	require.NoError(t, err)
	_, err = repository.Load("game-1")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "decode event envelope"))
}
