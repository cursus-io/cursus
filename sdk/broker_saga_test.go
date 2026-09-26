package sdk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBrokerSagaHistoryUsesStableRunSequenceIdentity(t *testing.T) {
	runtime := &BrokerSagaRuntime{
		config: BrokerSagaRuntimeConfig{
			SagaType: "orders", EnvironmentID: "test", ServiceName: "orders", Topics: DefaultBrokerSagaTopics(),
		},
	}
	state := &SagaState{ID: "order-42", Type: "orders", RunID: "de94b8eb-50c4-4a35-b324-59b9318af658"}
	input := BrokerSagaInput{
		SagaID: "order-42", RunID: state.RunID, Topic: "orders.events", Partition: 2, Offset: 9007199254740993,
		Group: "orders-saga", Member: "member-1", Event: EventEnvelope{EventID: "event-order-42", EventType: "OrderCreated", AggregateType: "order", AggregateID: "order-42", AggregateVersion: 7},
	}
	now := time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC)
	history := runtime.materializeHistory(state, input, []BrokerSagaHistoryDraft{{EventType: SagaHistoryRunStarted}}, now)
	require.Len(t, history, 1)
	require.Equal(t, uint64(1), history[0].Sequence)
	require.Equal(t, brokerSagaUUID("history", "orders", "order-42", state.RunID, "1").String(), history[0].HistoryEventID)
	encoded, err := json.Marshal(history[0])
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, json.Unmarshal(encoded, &record))
	require.Equal(t, "1", record["sequence"])
	require.Equal(t, "9007199254740993", record["source_offset"])
	require.Equal(t, "7", record["aggregate_version"])
}

func TestBrokerSagaHistoryRetainsZeroSourcePosition(t *testing.T) {
	runtime := &BrokerSagaRuntime{config: BrokerSagaRuntimeConfig{SagaType: "orders", EnvironmentID: "test", ServiceName: "orders", Topics: DefaultBrokerSagaTopics()}}
	state := &SagaState{ID: "order-42", Type: "orders", RunID: "de94b8eb-50c4-4a35-b324-59b9318af658"}
	input := BrokerSagaInput{SagaID: state.ID, RunID: state.RunID, Topic: "orders.events", Group: "orders-saga", Member: "member-1", Event: EventEnvelope{EventID: "event-order-42", EventType: "OrderCreated"}}
	history := runtime.materializeHistory(state, input, []BrokerSagaHistoryDraft{{EventType: SagaHistoryRunStarted}}, time.Now().UTC())
	encoded, err := json.Marshal(history[0])
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, json.Unmarshal(encoded, &record))
	require.Equal(t, float64(0), record["source_partition"])
	require.Equal(t, "0", record["source_offset"])
}

func TestBrokerSagaTransactionIDScopesIndependentConsumersAndRuns(t *testing.T) {
	runtime := &BrokerSagaRuntime{config: BrokerSagaRuntimeConfig{SagaType: "orders", ServiceName: "orders"}}
	input := BrokerSagaInput{SagaID: "order-42", RunID: "run-1", Topic: "orders.events", Partition: 0, Offset: 0, Group: "orders-a"}
	first := runtime.transactionID(input, "apply")
	input.Group = "orders-b"
	require.NotEqual(t, first, runtime.transactionID(input, "apply"))
	input.Group = "orders-a"
	input.RunID = "run-2"
	require.NotEqual(t, first, runtime.transactionID(input, "apply"))
}

func TestBrokerSagaTopicsRejectReservedTopics(t *testing.T) {
	config := BrokerSagaRuntimeConfig{
		SagaType: "orders", EnvironmentID: "test", ServiceName: "orders",
		Topics: BrokerSagaTopics{Inbox: "__inbox", State: "saga-state", Commands: "saga-commands", History: "saga-history"},
	}
	require.Error(t, config.validate())
}
