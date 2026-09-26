package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBrokerSagaRuntimeAgainstBroker verifies the real broker transaction
// boundary: state-stream append, command/history publication, source offset
// commit, and duplicate input acknowledgement happen atomically.
func TestBrokerSagaRuntimeAgainstBroker(t *testing.T) {
	rawAddrs := strings.TrimSpace(os.Getenv("CURSUS_INTEGRATION_ADDRS"))
	if rawAddrs == "" {
		t.Skip("set CURSUS_INTEGRATION_ADDRS to run broker saga integration validation")
	}
	addrs := strings.Split(rawAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topics := BrokerSagaTopics{
		Inbox: "sdk-saga-inbox-" + suffix, State: "sdk-saga-state-" + suffix,
		Commands: "sdk-saga-commands-" + suffix, History: "sdk-saga-history-" + suffix,
	}
	group := "sdk-saga-group-" + suffix
	config := NewDefaultConsumerConfig()
	config.BrokerAddrs = addrs
	client, err := NewConsumerClient(config)
	require.NoError(t, err)

	for _, topic := range []string{topics.Inbox, topics.Commands, topics.History} {
		require.NoError(t, createSagaIntegrationTopic(client, topic, false))
	}
	require.NoError(t, createSagaIntegrationTopic(client, topics.State, true))
	t.Cleanup(func() {
		for _, topic := range []string{topics.Inbox, topics.State, topics.Commands, topics.History} {
			_, _ = integrationOKCommand(client, "DELETE topic="+topic)
		}
	})

	// The source records make the transactional offset commit observable.
	require.NoError(t, publishSagaIntegrationInput(client, topics.Inbox, "OrderCreated", "source-1", 1))
	require.NoError(t, publishSagaIntegrationInput(client, topics.Inbox, "ReserveSucceeded", "source-2", 2))
	generation, member := integrationJoinGroup(t, client, topics.Inbox, group)

	store := NewEventStore(addrs[0], topics.State, "sdk-saga-state-producer-"+suffix)
	t.Cleanup(func() { _ = store.Close() })
	runtime, err := NewBrokerSagaRuntime(BrokerSagaRuntimeConfig{
		SagaType: "orders", EnvironmentID: "integration", ServiceName: "go-sdk", Topics: topics,
	}, client, store)
	require.NoError(t, err)

	runID := "b9ce0d32-8b83-4bd4-9d61-9e163b1b19d8"
	first := BrokerSagaInput{
		SagaID: "order-42", RunID: runID, Topic: topics.Inbox, Partition: 0, Offset: 0, Group: group, Member: member, Generation: generation,
		Event: EventEnvelope{EventID: "source-1", EventType: "OrderCreated", AggregateType: "order", AggregateID: "order-42", AggregateVersion: 1, CorrelationID: "order-42", OccurredAt: time.Now().UTC()},
	}
	require.NoError(t, runtime.Handle(context.Background(), first, func(_ context.Context, state *SagaState, _ EventEnvelope) ([]Command, []BrokerSagaHistoryDraft, error) {
		state.Status = SagaWaiting
		return []Command{{Type: "ReserveInventory", Payload: `{"order_id":"order-42"}`}}, []BrokerSagaHistoryDraft{{EventType: SagaHistoryStepCompleted, StepID: "reserve"}, {EventType: SagaHistoryRunWaiting}}, nil
	}))

	state := readSagaIntegrationState(t, store, runtime.StreamKey(first.SagaID, runID))
	require.Equal(t, uint64(1), state.State.Version)
	require.Equal(t, []string{"source-1"}, state.ProcessedEventIDs)
	assertSagaIntegrationOffset(t, client, topics.Inbox, group, 1)
	assertSagaIntegrationPublished(t, client, addrs, topics.Commands, "command")
	assertSagaIntegrationPublished(t, client, addrs, topics.History, "history")

	// Redelivery must only re-commit the same source offset; it cannot append a
	// second state transition or duplicate immutable history/command records.
	require.NoError(t, runtime.Handle(context.Background(), first, func(context.Context, *SagaState, EventEnvelope) ([]Command, []BrokerSagaHistoryDraft, error) {
		t.Fatal("duplicate input invoked saga handler")
		return nil, nil, nil
	}))
	state = readSagaIntegrationState(t, store, runtime.StreamKey(first.SagaID, runID))
	require.Equal(t, uint64(1), state.State.Version)

	second := first
	second.Offset, second.Event = 1, EventEnvelope{EventID: "source-2", EventType: "ReserveSucceeded", AggregateType: "order", AggregateID: "order-42", AggregateVersion: 2, CorrelationID: "order-42", OccurredAt: time.Now().UTC()}
	require.NoError(t, runtime.Handle(context.Background(), second, func(_ context.Context, state *SagaState, _ EventEnvelope) ([]Command, []BrokerSagaHistoryDraft, error) {
		state.Status, state.Outcome = SagaCompleted, SagaOutcomeSucceeded
		return nil, []BrokerSagaHistoryDraft{{EventType: SagaHistoryCommandSucceeded, StepID: "reserve"}, {EventType: SagaHistoryRunCompleted}}, nil
	}))
	state = readSagaIntegrationState(t, store, runtime.StreamKey(second.SagaID, runID))
	require.Equal(t, uint64(2), state.State.Version)
	require.Equal(t, []string{"source-1", "source-2"}, state.ProcessedEventIDs)
	require.Equal(t, SagaOutcomeSucceeded, state.State.Outcome)
	assertSagaIntegrationOffset(t, client, topics.Inbox, group, 2)
}

func createSagaIntegrationTopic(client *ConsumerClient, topic string, eventSourcing bool) error {
	command := fmt.Sprintf("CREATE topic=%s partitions=1", topic)
	if eventSourcing {
		command += " event_sourcing=true"
	}
	_, err := integrationOKCommand(client, command)
	return err
}

func publishSagaIntegrationInput(client *ConsumerClient, topic, eventType, payload string, sequence uint64) error {
	_, err := integrationOKCommand(client, fmt.Sprintf("PUBLISH topic=%s partition=0 producerId=sdk-saga-input seqNum=%d event_type=%s schema_version=1 message=%s", topic, sequence, eventType, payload))
	return err
}

func readSagaIntegrationState(t *testing.T, store *EventStore, key string) BrokerSagaStateRecord {
	t.Helper()
	stream, err := store.ReadStream(key)
	require.NoError(t, err)
	require.NotEmpty(t, stream.Events)
	var state BrokerSagaStateRecord
	require.NoError(t, json.Unmarshal([]byte(stream.Events[len(stream.Events)-1].Payload), &state))
	return state
}

func assertSagaIntegrationOffset(t *testing.T, client *ConsumerClient, topic, group string, want uint64) {
	t.Helper()
	response, err := integrationOKCommand(client, fmt.Sprintf("FETCH_OFFSET topic=%s partition=0 group=%s", topic, group))
	require.NoError(t, err)
	fields, err := parseOKResponse(response)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(want, 10), fields["offset"])
}

func assertSagaIntegrationPublished(t *testing.T, client *ConsumerClient, addrs []string, topic, name string) {
	t.Helper()
	group := "sdk-saga-verify-" + name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	generation, member := integrationJoinGroup(t, client, topic, group)
	messages := integrationConsumePartition(t, client, addrs, topic, group, member, generation)
	require.NotEmpty(t, messages, "expected broker saga %s publication", name)
}
