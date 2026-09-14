package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

// SagaHistoryEvent is the language-independent, append-only v1 execution
// record. Sequence is local to one run; occurred_at is never a cross-service
// ordering guarantee.
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
	SourcePartition      int       `json:"source_partition,omitempty"`
	SourceOffset         uint64    `json:"source_offset,string,omitempty"`
	AggregateType        string    `json:"aggregate_type,omitempty"`
	AggregateID          string    `json:"aggregate_id,omitempty"`
	AggregateVersion     uint64    `json:"aggregate_version,string,omitempty"`
	Payload              string    `json:"payload,omitempty"`
	Error                string    `json:"error,omitempty"`
}

// SagaHistoryStore persists an event in the service-owned database. It must be
// called through SagaTransaction when an application needs atomic state,
// inbox, outbox, and history persistence.
type SagaHistoryStore interface {
	AppendSagaHistory(context.Context, SagaHistoryEvent) error
}

// SagaTransactionStores are the transaction-scoped adapters supplied by a
// service's database integration.
type SagaTransactionStores struct {
	Inbox   InboxStore
	State   SagaStore
	Outbox  OutboxStore
	History SagaHistoryStore
}

// SagaTransaction is implemented by a real service DB adapter (for example a
// PostgreSQL transaction). It must commit only when fn returns nil.
type SagaTransaction interface {
	WithinSagaTransaction(context.Context, func(context.Context, SagaTransactionStores) error) error
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

func newSagaRunID() string          { return uuid.NewString() }
func newSagaHistoryEventID() string { return uuid.NewString() }
