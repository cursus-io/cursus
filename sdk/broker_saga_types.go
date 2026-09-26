package sdk

import (
	"fmt"
	"time"
)

const (
	SagaRunning      = "RUNNING"
	SagaWaiting      = "WAITING"
	SagaCompleted    = "COMPLETED"
	SagaCompensating = "COMPENSATING"
	SagaFailed       = "FAILED"
)

// SagaState is the broker-native state payload reconstructed from the Saga
// state topic. It deliberately has no database adapter or local outbox API.
type SagaState struct {
	ID             string
	Type           string
	AssociationKey string
	CorrelationID  string
	Status         string
	Step           string
	Data           string
	RetryCount     int
	LastError      string
	RunID          string
	NextSequence   uint64
	Outcome        string
	UpdatedAt      time.Time
	Version        uint64
	Effects        map[string]EffectState
}

const (
	EffectEnqueued  = "ENQUEUED"
	EffectSucceeded = "SUCCEEDED"
	EffectFailed    = "FAILED"
)

type EffectState struct {
	ID        string
	Step      string
	Status    string
	CommandID string
	Published bool
	Attempts  int
	LastError string
	UpdatedAt time.Time
}

// Command is an application command emitted in the broker transaction with
// the corresponding Saga state and immutable execution history records.
type Command struct {
	ID            string
	EffectID      string
	Type          string
	SagaType      string
	SagaID        string
	CorrelationID string
	CausationID   string
	Payload       string
}

const SagaHistorySchemaV1 uint32 = 1

const (
	SagaHistoryRunStarted            = "run.started"
	SagaHistoryRunWaiting            = "run.waiting"
	SagaHistoryStepStarted           = "step.started"
	SagaHistoryStepCompleted         = "step.completed"
	SagaHistoryStepFailed            = "step.failed"
	SagaHistoryCommandEnqueued       = "command.enqueued"
	SagaHistoryCommandPublished      = "command.published"
	SagaHistoryCommandSucceeded      = "command.succeeded"
	SagaHistoryCommandFailed         = "command.failed"
	SagaHistoryCompensationStarted   = "compensation.started"
	SagaHistoryCompensationCompleted = "compensation.completed"
	SagaHistoryCompensationFailed    = "compensation.failed"
	SagaHistoryRunCompleted          = "run.completed"
	SagaHistoryRunFailed             = "run.failed"
	SagaHistoryRunCompensated        = "run.compensated"
)

const (
	SagaOutcomeSucceeded   = "SUCCEEDED"
	SagaOutcomeCompensated = "COMPENSATED"
	SagaOutcomeFailed      = "FAILED"
)

// SagaHistoryEvent is the language-independent append-only v1 contract.
// Sequence, source_offset, and aggregate_version are JSON strings.
type SagaHistoryEvent struct {
	HistorySchemaVersion uint32    `json:"history_schema_version"`
	HistoryEventID       string    `json:"history_event_id"`
	EnvironmentID        string    `json:"environment_id"`
	ServiceName          string    `json:"service_name"`
	SagaType             string    `json:"saga_type"`
	SagaID               string    `json:"saga_id"`
	RunID                string    `json:"run_id"`
	Sequence             uint64    `json:"sequence,string"`
	EventType            string    `json:"event_type"`
	OccurredAt           time.Time `json:"occurred_at"`
	RecordedAt           time.Time `json:"recorded_at"`
	StepID               string    `json:"step_id,omitempty"`
	Attempt              uint32    `json:"attempt,omitempty"`
	CommandID            string    `json:"command_id,omitempty"`
	EffectID             string    `json:"effect_id,omitempty"`
	SourceEventID        string    `json:"source_event_id,omitempty"`
	CorrelationID        string    `json:"correlation_id,omitempty"`
	CausationID          string    `json:"causation_id,omitempty"`
	SourceTopic          string    `json:"source_topic,omitempty"`
	SourcePartition      int       `json:"source_partition"`
	SourceOffset         uint64    `json:"source_offset,string"`
	AggregateType        string    `json:"aggregate_type,omitempty"`
	AggregateID          string    `json:"aggregate_id,omitempty"`
	AggregateVersion     uint64    `json:"aggregate_version,string,omitempty"`
	Payload              string    `json:"payload,omitempty"`
	Error                string    `json:"error,omitempty"`
}

type SagaHistoryOptions struct {
	EnvironmentID string
	ServiceName   string
}

func (o SagaHistoryOptions) validate() error {
	if o.EnvironmentID == "" || o.ServiceName == "" {
		return fmt.Errorf("saga history requires environment and service name")
	}
	return nil
}
