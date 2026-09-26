package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// BrokerSagaTopics contains the public broker topics used by the broker-native
// Saga runtime. None of these names are reserved Cursus topics; applications
// may configure them per environment.
type BrokerSagaTopics struct {
	Inbox    string
	State    string
	Commands string
	History  string
}

func DefaultBrokerSagaTopics() BrokerSagaTopics {
	return BrokerSagaTopics{
		Inbox:    "cursus.saga-inbox.v1",
		State:    "cursus.saga-state.v1",
		Commands: "cursus.saga-commands.v1",
		History:  "cursus.saga-history.v1",
	}
}

func (t BrokerSagaTopics) validate() error {
	for label, value := range map[string]string{
		"inbox": t.Inbox, "state": t.State, "commands": t.Commands, "history": t.History,
	} {
		if err := validateSDKTopicName(value); err != nil {
			return fmt.Errorf("broker saga %s topic: %w", label, err)
		}
		if strings.HasPrefix(value, "__") {
			return fmt.Errorf("broker saga %s topic must not be reserved", label)
		}
	}
	return nil
}

// BrokerSagaRuntimeConfig identifies one service's broker-native Saga
// runtime. State is always persisted to Topics.State, never to a service DB.
type BrokerSagaRuntimeConfig struct {
	SagaType      string
	EnvironmentID string
	ServiceName   string
	Topics        BrokerSagaTopics
}

func (c BrokerSagaRuntimeConfig) validate() error {
	if c.SagaType == "" || strings.ContainsAny(c.SagaType, " \t\r\n") {
		return fmt.Errorf("broker saga type is required and must not contain whitespace")
	}
	if err := (SagaHistoryOptions{EnvironmentID: c.EnvironmentID, ServiceName: c.ServiceName}).validate(); err != nil {
		return err
	}
	return c.Topics.validate()
}

// BrokerSagaInput identifies one consumed inbox message and the consumer
// group membership that is allowed to commit it transactionally. RunID is
// deliberately required: redelivery never invents a new Saga run.
type BrokerSagaInput struct {
	SagaID     string
	RunID      string
	Event      EventEnvelope
	Topic      string
	Partition  int
	Offset     uint64
	Group      string
	Member     string
	Generation int
}

func (i BrokerSagaInput) validate() error {
	if i.SagaID == "" || i.RunID == "" || strings.ContainsAny(i.SagaID+i.RunID, " \t\r\n") {
		return fmt.Errorf("broker saga ID and explicit run ID are required and must not contain whitespace")
	}
	if i.Event.EventID == "" || i.Event.EventType == "" {
		return fmt.Errorf("broker saga input event identity is incomplete")
	}
	if err := validateSDKTopicName(i.Topic); err != nil {
		return fmt.Errorf("broker saga input topic: %w", err)
	}
	if i.Partition < 0 || i.Group == "" || i.Member == "" || i.Generation < 0 {
		return fmt.Errorf("broker saga transactional input metadata is incomplete")
	}
	return nil
}

// BrokerSagaHistoryDraft represents one deliberate execution-history fact.
// In particular, command.succeeded and command.failed must only be supplied
// by a handler that has received an explicit business-result event.
type BrokerSagaHistoryDraft struct {
	EventType string
	StepID    string
	Attempt   uint32
	Command   Command
	Payload   string
	Error     string
}

// BrokerSagaTransitionHandler mutates the recovered Saga state and returns
// commands plus any explicit history facts. The runtime adds command.enqueued
// facts for returned commands; it never infers command success from publishing.
type BrokerSagaTransitionHandler func(context.Context, *SagaState, EventEnvelope) ([]Command, []BrokerSagaHistoryDraft, error)

// BrokerSagaStateRecord is the versioned event stored in the state stream.
// ProcessedEventIDs is the broker-native inbox: it makes duplicate delivery
// harmless without a database table. Deployments may compact/snapshot this
// record once their event-store snapshot policy is enabled.
type BrokerSagaStateRecord struct {
	SchemaVersion     uint32    `json:"schema_version"`
	SagaType          string    `json:"saga_type"`
	SagaID            string    `json:"saga_id"`
	RunID             string    `json:"run_id"`
	State             SagaState `json:"state"`
	ProcessedEventIDs []string  `json:"processed_event_ids"`
	RecordedAt        time.Time `json:"recorded_at"`
}

// BrokerSagaCommandEnvelope is published with commands so effect executors can
// deduplicate by command_id before performing an external side effect.
type BrokerSagaCommandEnvelope struct {
	SchemaVersion uint32 `json:"schema_version"`
	CommandID     string `json:"command_id"`
	EffectID      string `json:"effect_id"`
	CommandType   string `json:"command_type"`
	SagaType      string `json:"saga_type"`
	SagaID        string `json:"saga_id"`
	RunID         string `json:"run_id"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	Payload       string `json:"payload,omitempty"`
}

// BrokerSagaRuntime is an opt-in DB-free Saga transaction boundary. It uses a
// broker transaction to atomically append state, emit commands and history,
// and commit the source inbox offset.
type BrokerSagaRuntime struct {
	config     BrokerSagaRuntimeConfig
	client     *ConsumerClient
	stateStore *EventStore
	now        func() time.Time
}

func NewBrokerSagaRuntime(config BrokerSagaRuntimeConfig, client *ConsumerClient, stateStore *EventStore) (*BrokerSagaRuntime, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if client == nil || stateStore == nil {
		return nil, fmt.Errorf("broker saga consumer client and state store are required")
	}
	return &BrokerSagaRuntime{config: config, client: client, stateStore: stateStore, now: time.Now}, nil
}

// StreamKey returns the sole state stream for a Saga run. A new execution must
// use a new explicit run ID, so no duplicate input can create a new stream.
func (r *BrokerSagaRuntime) StreamKey(sagaID, runID string) string {
	return r.config.SagaType + ":" + sagaID + ":" + runID
}

// Handle applies one inbox event. The caller supplies group metadata from its
// active consumer assignment and must configure that consumer with
// EnableAutoCommit=false.
func (r *BrokerSagaRuntime) Handle(ctx context.Context, input BrokerSagaInput, handler BrokerSagaTransitionHandler) error {
	if err := input.validate(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("broker saga transition handler is required")
	}
	record, version, err := r.load(input.SagaID, input.RunID)
	if err != nil {
		return err
	}
	if containsSagaInput(record.ProcessedEventIDs, input.Event.EventID) {
		return r.commitDuplicate(input)
	}

	newRun := version == 0
	if newRun {
		record = BrokerSagaStateRecord{
			SchemaVersion: 1,
			SagaType:      r.config.SagaType,
			SagaID:        input.SagaID,
			RunID:         input.RunID,
			State: SagaState{ID: input.SagaID, Type: r.config.SagaType, AssociationKey: input.SagaID,
				CorrelationID: input.Event.CorrelationID, RunID: input.RunID, Status: SagaRunning, Effects: map[string]EffectState{}},
		}
	}
	if record.State.Effects == nil {
		record.State.Effects = make(map[string]EffectState)
	}
	// A failed transition must not leak mutations made by the handler into the
	// failure record. Only a successful handler result becomes the next state.
	transitionState := cloneBrokerSagaState(record.State)
	commands, drafts, handleErr := handler(ctx, &transitionState, input.Event)
	if handleErr != nil {
		if !newRun {
			_ = r.recordFailure(ctx, input, record, version, handleErr)
		}
		return handleErr
	}
	record.State = transitionState

	now := r.now().UTC()
	if newRun {
		drafts = append([]BrokerSagaHistoryDraft{{EventType: SagaHistoryRunStarted}}, drafts...)
	}
	for index := range commands {
		command := r.prepareCommand(commands[index], &record.State, input.Event.EventID, index)
		commands[index] = command
		drafts = append(drafts, BrokerSagaHistoryDraft{EventType: SagaHistoryCommandEnqueued, StepID: command.Type, Command: command, Payload: command.Payload})
	}
	record.ProcessedEventIDs = append(record.ProcessedEventIDs, input.Event.EventID)
	record.RecordedAt = now
	record.State.Version = version + 1
	record.State.UpdatedAt = now
	history := r.materializeHistory(&record.State, input, drafts, now)
	return r.commit(input, record, version+1, commands, history, true)
}

func (r *BrokerSagaRuntime) load(sagaID, runID string) (BrokerSagaStateRecord, uint64, error) {
	stream, err := r.stateStore.ReadStream(r.StreamKey(sagaID, runID))
	if err != nil {
		return BrokerSagaStateRecord{}, 0, fmt.Errorf("read broker saga state stream: %w", err)
	}
	if len(stream.Events) == 0 {
		return BrokerSagaStateRecord{}, 0, nil
	}
	last := stream.Events[len(stream.Events)-1]
	var record BrokerSagaStateRecord
	if err := json.Unmarshal([]byte(last.Payload), &record); err != nil {
		return BrokerSagaStateRecord{}, 0, fmt.Errorf("decode broker saga state: %w", err)
	}
	if record.SchemaVersion != 1 || record.SagaType != r.config.SagaType || record.SagaID != sagaID || record.RunID != runID {
		return BrokerSagaStateRecord{}, 0, fmt.Errorf("broker saga state stream identity mismatch")
	}
	return record, last.Version, nil
}

func (r *BrokerSagaRuntime) prepareCommand(command Command, state *SagaState, causationID string, index int) Command {
	if command.EffectID == "" {
		command.EffectID = fmt.Sprintf("%s:%d", causationID, index)
	}
	if command.ID == "" {
		command.ID = brokerSagaUUID("command", r.config.SagaType, state.ID, state.RunID, command.EffectID).String()
	}
	if command.SagaType == "" {
		command.SagaType = r.config.SagaType
	}
	if command.SagaID == "" {
		command.SagaID = state.ID
	}
	if command.CorrelationID == "" {
		command.CorrelationID = state.CorrelationID
	}
	if command.CausationID == "" {
		command.CausationID = causationID
	}
	if state.Effects == nil {
		state.Effects = make(map[string]EffectState)
	}
	effect := state.Effects[command.EffectID]
	effect.ID = command.EffectID
	effect.Step = command.Type
	effect.Status = EffectEnqueued
	effect.CommandID = command.ID
	effect.Attempts++
	effect.LastError = ""
	effect.UpdatedAt = r.now().UTC()
	state.Effects[command.EffectID] = effect
	return command
}

func (r *BrokerSagaRuntime) materializeHistory(state *SagaState, input BrokerSagaInput, drafts []BrokerSagaHistoryDraft, now time.Time) []SagaHistoryEvent {
	history := make([]SagaHistoryEvent, 0, len(drafts))
	for _, draft := range drafts {
		state.NextSequence++
		command := draft.Command
		history = append(history, SagaHistoryEvent{
			HistorySchemaVersion: SagaHistorySchemaV1,
			HistoryEventID:       brokerSagaUUID("history", r.config.SagaType, state.ID, state.RunID, fmt.Sprintf("%d", state.NextSequence)).String(),
			EnvironmentID:        r.config.EnvironmentID,
			ServiceName:          r.config.ServiceName,
			SagaType:             r.config.SagaType,
			SagaID:               state.ID,
			RunID:                state.RunID,
			Sequence:             state.NextSequence,
			EventType:            draft.EventType,
			OccurredAt:           now,
			RecordedAt:           now,
			StepID:               draft.StepID,
			Attempt:              draft.Attempt,
			CommandID:            command.ID,
			EffectID:             command.EffectID,
			SourceEventID:        input.Event.EventID,
			CorrelationID:        input.Event.CorrelationID,
			CausationID:          input.Event.CausationID,
			SourceTopic:          input.Topic,
			SourcePartition:      input.Partition,
			SourceOffset:         input.Offset,
			AggregateType:        input.Event.AggregateType,
			AggregateID:          input.Event.AggregateID,
			AggregateVersion:     input.Event.AggregateVersion,
			Payload:              draft.Payload,
			Error:                draft.Error,
		})
	}
	return history
}

func (r *BrokerSagaRuntime) commitDuplicate(input BrokerSagaInput) error {
	return r.commit(input, BrokerSagaStateRecord{}, 0, nil, nil, false)
}

func (r *BrokerSagaRuntime) commit(input BrokerSagaInput, record BrokerSagaStateRecord, expectedVersion uint64, commands []Command, history []SagaHistoryEvent, appendState bool) (err error) {
	producer, err := r.client.NewTransactionalProducer(r.transactionID(input, "apply"))
	if err != nil {
		return fmt.Errorf("create broker saga transaction producer: %w", err)
	}
	if err := producer.Begin(); err != nil {
		return fmt.Errorf("begin broker saga transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = producer.Abort()
		}
	}()

	// Idempotent producer sequences are fenced per topic-partition. A Saga
	// transaction can atomically touch state, command, and history topics, so
	// one global counter would make the first write to a later topic start at
	// sequence 2 and be rejected by its partition.
	sequences := make(map[string]uint64)
	nextMessage := func(topic, payload, key, eventType string) Message {
		sequences[topic]++
		return Message{SeqNum: sequences[topic], Payload: payload, Key: key, EventType: eventType, SchemaVersion: 1}
	}
	if appendState {
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return fmt.Errorf("marshal broker saga state: %w", marshalErr)
		}
		if err := producer.AppendStream(r.config.Topics.State, r.StreamKey(record.SagaID, record.RunID), expectedVersion, nextMessage(r.config.Topics.State, string(payload), r.StreamKey(record.SagaID, record.RunID), "saga.state.transitioned")); err != nil {
			return fmt.Errorf("append broker saga state: %w", err)
		}
	}
	for _, command := range commands {
		payload, marshalErr := json.Marshal(BrokerSagaCommandEnvelope{SchemaVersion: 1, CommandID: command.ID, EffectID: command.EffectID, CommandType: command.Type, SagaType: r.config.SagaType, SagaID: record.SagaID, RunID: record.RunID, CorrelationID: command.CorrelationID, CausationID: command.CausationID, Payload: command.Payload})
		if marshalErr != nil {
			return fmt.Errorf("marshal broker saga command: %w", marshalErr)
		}
		if err := producer.Publish(r.config.Topics.Commands, -1, nextMessage(r.config.Topics.Commands, string(payload), command.ID, "saga.command.enqueued")); err != nil {
			return fmt.Errorf("publish broker saga command: %w", err)
		}
	}
	for _, event := range history {
		payload, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return fmt.Errorf("marshal broker saga history: %w", marshalErr)
		}
		if err := producer.Publish(r.config.Topics.History, -1, nextMessage(r.config.Topics.History, string(payload), event.HistoryEventID, event.EventType)); err != nil {
			return fmt.Errorf("publish broker saga history: %w", err)
		}
	}
	if err := producer.SendOffsets(input.Topic, input.Group, input.Member, input.Generation, map[int]uint64{input.Partition: input.Offset + 1}); err != nil {
		return fmt.Errorf("stage broker saga inbox offset: %w", err)
	}
	if err := producer.Commit(); err != nil {
		return fmt.Errorf("commit broker saga transaction: %w", err)
	}
	committed = true
	return nil
}

func (r *BrokerSagaRuntime) recordFailure(ctx context.Context, input BrokerSagaInput, record BrokerSagaStateRecord, version uint64, cause error) error {
	if cause == nil {
		return nil
	}
	now := r.now().UTC()
	record.State.RetryCount++
	record.State.LastError = cause.Error()
	record.State.Version = version + 1
	record.State.UpdatedAt = now
	record.RecordedAt = now
	attempt := uint32(math.MaxUint32)
	if record.State.RetryCount >= 0 && record.State.RetryCount < math.MaxUint32 {
		attempt = uint32(record.State.RetryCount) // #nosec G115 -- bounds checked
	}
	history := r.materializeHistory(&record.State, input, []BrokerSagaHistoryDraft{{EventType: SagaHistoryStepFailed, StepID: record.State.Step, Attempt: attempt, Error: cause.Error()}}, now)
	// Do not acknowledge the inbox offset: the source record is retried, while
	// this separately committed failure remains an immutable diagnostic fact.
	return r.commitStateOnly(input, record, version+1, history)
}

func (r *BrokerSagaRuntime) commitStateOnly(input BrokerSagaInput, record BrokerSagaStateRecord, expectedVersion uint64, history []SagaHistoryEvent) (err error) {
	producer, err := r.client.NewTransactionalProducer(r.transactionID(input, "failure"))
	if err != nil {
		return err
	}
	if err := producer.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = producer.Abort()
		}
	}()
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := producer.AppendStream(r.config.Topics.State, r.StreamKey(record.SagaID, record.RunID), expectedVersion, Message{SeqNum: 1, Payload: string(payload), Key: r.StreamKey(record.SagaID, record.RunID), EventType: "saga.state.failed", SchemaVersion: 1}); err != nil {
		return err
	}
	for index, event := range history {
		payload, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return marshalErr
		}
		if err := producer.Publish(r.config.Topics.History, -1, Message{SeqNum: uint64(index + 1), Payload: string(payload), Key: event.HistoryEventID, EventType: event.EventType, SchemaVersion: 1}); err != nil {
			return err
		}
	}
	if err := producer.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (r *BrokerSagaRuntime) transactionID(input BrokerSagaInput, phase string) string {
	return "saga-" + brokerSagaUUID("transaction", r.config.ServiceName, r.config.SagaType, input.Group, input.SagaID, input.RunID, input.Topic, fmt.Sprintf("%d", input.Partition), fmt.Sprintf("%d", input.Offset), phase).String()
}

func cloneBrokerSagaState(state SagaState) SagaState {
	clone := state
	if state.Effects == nil {
		return clone
	}
	clone.Effects = make(map[string]EffectState, len(state.Effects))
	for id, effect := range state.Effects {
		clone.Effects[id] = effect
	}
	return clone
}

func containsSagaInput(ids []string, eventID string) bool {
	for _, id := range ids {
		if id == eventID {
			return true
		}
	}
	return false
}

var brokerSagaNamespace = uuid.MustParse("2ce850f6-b151-5e5a-a160-6b8d82527d54")

func brokerSagaUUID(parts ...string) uuid.UUID {
	return uuid.NewSHA1(brokerSagaNamespace, []byte(strings.Join(parts, "\x00")))
}
