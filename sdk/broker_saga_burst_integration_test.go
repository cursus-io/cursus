package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBrokerSagaRuntimeBurstAgainstBroker exercises the multi-group backlog
// that a two-message, single-group transaction test cannot cover. Run it with
// CURSUS_INTEGRATION_ADDRS against a three-broker Docker cluster. Set
// CURSUS_INTEGRATION_BURST_SIZE=32 for the sustained commit-deadline regression.
func TestBrokerSagaRuntimeBurstAgainstBroker(t *testing.T) {
	rawAddrs := strings.TrimSpace(os.Getenv("CURSUS_INTEGRATION_ADDRS"))
	if rawAddrs == "" {
		t.Skip("set CURSUS_INTEGRATION_ADDRS to run broker saga burst validation")
	}
	addrs := strings.Split(rawAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	const groupCount = 4
	inputsPerGroup := 8
	if raw := os.Getenv("CURSUS_INTEGRATION_BURST_SIZE"); raw != "" {
		var parseErr error
		inputsPerGroup, parseErr = strconv.Atoi(raw)
		require.NoError(t, parseErr)
		require.Greater(t, inputsPerGroup, 0)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topics := BrokerSagaTopics{
		Inbox: "sdk-burst-inbox-" + suffix, State: "sdk-burst-state-" + suffix,
		Commands: "sdk-burst-commands-" + suffix, History: "sdk-burst-history-" + suffix,
	}
	config := NewDefaultConsumerConfig()
	config.BrokerAddrs = addrs
	admin, err := NewConsumerClient(config)
	require.NoError(t, err)
	for _, topic := range []string{topics.Inbox, topics.Commands, topics.History} {
		require.NoError(t, createSagaIntegrationTopic(admin, topic, false))
	}
	require.NoError(t, createSagaIntegrationTopic(admin, topics.State, true))
	t.Cleanup(func() {
		for _, topic := range []string{topics.Inbox, topics.State, topics.Commands, topics.History} {
			_, _ = integrationOKCommand(admin, "DELETE topic="+topic)
		}
	})
	for i := 0; i < inputsPerGroup; i++ {
		require.NoError(t, publishSagaIntegrationInput(admin, topics.Inbox, "OrderCreated", fmt.Sprintf("source-%d", i), uint64(i+1)))
	}

	type worker struct {
		group      string
		member     string
		generation int
		runtime    *BrokerSagaRuntime
		store      *EventStore
	}
	workers := make([]worker, groupCount)
	for i := range workers {
		group := fmt.Sprintf("sdk-burst-group-%s-%d", suffix, i)
		generation, member := integrationJoinGroup(t, admin, topics.Inbox, group)
		clientConfig := NewDefaultConsumerConfig()
		clientConfig.BrokerAddrs = addrs
		client, clientErr := NewConsumerClient(clientConfig)
		require.NoError(t, clientErr)
		store := NewEventStore(addrs[0], topics.State, fmt.Sprintf("sdk-burst-state-%s-%d", suffix, i))
		t.Cleanup(func() { _ = store.Close() })
		runtime, runtimeErr := NewBrokerSagaRuntime(BrokerSagaRuntimeConfig{
			SagaType: "orders", EnvironmentID: "integration", ServiceName: "burst-worker", Topics: topics,
		}, client, store)
		require.NoError(t, runtimeErr)
		workers[i] = worker{group: group, member: member, generation: generation, runtime: runtime, store: store}
	}
	heartbeatCtx, stopHeartbeats := context.WithCancel(context.Background())
	defer stopHeartbeats()
	var heartbeatWG sync.WaitGroup
	heartbeatErrors := make(chan error, groupCount)
	for _, w := range workers {
		heartbeatWG.Add(1)
		go func(w worker) {
			defer heartbeatWG.Done()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					_, err := integrationOKCommand(admin, fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", topics.Inbox, w.group, w.member, w.generation))
					if err != nil {
						heartbeatErrors <- fmt.Errorf("group %s heartbeat: %w", w.group, err)
						return
					}
				}
			}
		}(w)
	}

	var wg sync.WaitGroup
	errors := make(chan error, groupCount)
	for i, w := range workers {
		wg.Add(1)
		go func(i int, w worker) {
			defer wg.Done()
			for offset := 0; offset < inputsPerGroup; offset++ {
				id := fmt.Sprintf("order-%s-%d-%d", suffix, i, offset)
				input := BrokerSagaInput{
					SagaID: id, RunID: id, Topic: topics.Inbox, Partition: 0, Offset: uint64(offset),
					Group: w.group, Member: w.member, Generation: w.generation,
					Event: EventEnvelope{EventID: fmt.Sprintf("source-%d", offset), EventType: "OrderCreated",
						AggregateType: "order", AggregateID: id, AggregateVersion: 1,
						CorrelationID: id, OccurredAt: time.Now().UTC()},
				}
				started := time.Now()
				err := w.runtime.Handle(context.Background(), input, func(_ context.Context, state *SagaState, _ EventEnvelope) ([]Command, []BrokerSagaHistoryDraft, error) {
					state.Status = SagaWaiting
					return []Command{{Type: "ReserveInventory", Payload: fmt.Sprintf(`{"order_id":%q}`, id)}},
						[]BrokerSagaHistoryDraft{{EventType: SagaHistoryStepCompleted, StepID: "reserve"}, {EventType: SagaHistoryRunWaiting}}, nil
				})
				if err != nil {
					errors <- fmt.Errorf("group %d offset %d after %s: %w", i, offset, time.Since(started), err)
					return
				}
			}
		}(i, w)
	}
	wg.Wait()
	stopHeartbeats()
	heartbeatWG.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	close(heartbeatErrors)
	for err := range heartbeatErrors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	for _, w := range workers {
		assertSagaIntegrationOffset(t, admin, topics.Inbox, w.group, uint64(inputsPerGroup))
		response, err := integrationOKCommand(admin, "GROUP_STATUS group="+w.group)
		require.NoError(t, err)
		var status struct {
			Generation  int `json:"generation"`
			MemberCount int `json:"member_count"`
		}
		require.NoError(t, json.Unmarshal([]byte(response), &status))
		require.Equal(t, w.generation, status.Generation, "healthy Saga processing must not rebalance the group")
		require.Equal(t, 1, status.MemberCount, "one partition needs one active member")
	}
}
