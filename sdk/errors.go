package sdk

import (
	"errors"
	"fmt"
	"strings"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

var (
	ErrProducerClosed          = errors.New("producer closed")
	ErrProducerOutcomeUnknown  = errors.New("producer delivery outcome unknown")
	ErrRequestOutcomeUnknown   = errors.New("request outcome unknown")
	ErrConsumerClosed          = errors.New("consumer closed")
	ErrConsumerRebalancing     = errors.New("consumer assignment is rebalancing")
	ErrConsumerHandlerRequired = errors.New("consumer message handler is required")
	ErrTopicNotFound           = errors.New("topic not found")
	ErrInvalidPartition        = errors.New("invalid partition")
	ErrNotLeader               = errors.New("not leader")
)

// RequestOutcomeUnknownError means a mutating request was fully written but
// its response could not be read. Callers must reconcile server state before
// retrying an operation that is not idempotent.
type RequestOutcomeUnknownError struct {
	Operation string
	Cause     error
}

func (e *RequestOutcomeUnknownError) Error() string {
	if e == nil {
		return ErrRequestOutcomeUnknown.Error()
	}
	return fmt.Sprintf("%s during %s: %v", ErrRequestOutcomeUnknown, e.Operation, e.Cause)
}

func (e *RequestOutcomeUnknownError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrRequestOutcomeUnknown}
	}
	return []error{ErrRequestOutcomeUnknown, e.Cause}
}

// ProducerOutcomeUnknownError means a non-idempotent publish may have reached
// the broker, but the SDK could not obtain a trustworthy acknowledgement. The
// caller must reconcile application state before deciding whether to publish
// the record again.
type ProducerOutcomeUnknownError struct {
	Partition int
	Stage     string
	Cause     error
}

func (e *ProducerOutcomeUnknownError) Error() string {
	if e == nil {
		return ErrProducerOutcomeUnknown.Error()
	}
	if e.Cause == nil {
		return fmt.Sprintf("%s for partition %d during %s", ErrProducerOutcomeUnknown, e.Partition, e.Stage)
	}
	return fmt.Sprintf("%s for partition %d during %s: %v", ErrProducerOutcomeUnknown, e.Partition, e.Stage, e.Cause)
}

func (e *ProducerOutcomeUnknownError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrProducerOutcomeUnknown}
	}
	return []error{ErrProducerOutcomeUnknown, e.Cause}
}

// ConsumerOffsetOutOfRangeError reports a retained offset that is no longer
// readable while AutoOffsetResetError is configured.
type ConsumerOffsetOutOfRangeError struct {
	Partition int
	Requested uint64
	Earliest  uint64
	Latest    uint64
}

func (e *ConsumerOffsetOutOfRangeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("consumer partition %d offset %d is out of range (earliest=%d latest=%d)", e.Partition, e.Requested, e.Earliest, e.Latest)
}

// ConsumerHandlerError reports a record handler that exhausted its bounded
// retry budget. The failed record is not committed.
type ConsumerHandlerError struct {
	Partition int
	Offset    uint64
	Attempts  int
	Cause     error
}

func (e *ConsumerHandlerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("consumer handler failed for partition %d offset %d after %d attempts: %v", e.Partition, e.Offset, e.Attempts, e.Cause)
}

func (e *ConsumerHandlerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type ErrorClass = wireprotocol.ErrorClass

const (
	ErrorClassAuthorization = wireprotocol.ErrorClassAuthorization
	ErrorClassAvailability  = wireprotocol.ErrorClassAvailability
	ErrorClassConflict      = wireprotocol.ErrorClassConflict
	ErrorClassFencing       = wireprotocol.ErrorClassFencing
	ErrorClassInternal      = wireprotocol.ErrorClassInternal
	ErrorClassNotFound      = wireprotocol.ErrorClassNotFound
	ErrorClassRouting       = wireprotocol.ErrorClassRouting
	ErrorClassValidation    = wireprotocol.ErrorClassValidation
)

type BrokerError struct {
	Code      string
	Class     ErrorClass
	Retryable bool
	Message   string
	Fields    map[string]string
}

func (e *BrokerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := e.Message
	if message == "" && e.Fields != nil {
		message = e.Fields["reason"]
	}
	if message == "" {
		return fmt.Sprintf("broker error %s (%s)", e.Code, e.Class)
	}
	return fmt.Sprintf("broker error %s (%s): %s", e.Code, e.Class, message)
}

func (e *BrokerError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrTopicNotFound:
		return strings.EqualFold(e.Code, "topic_not_found") || strings.EqualFold(e.Code, "TOPIC_NOT_FOUND")
	case ErrInvalidPartition:
		return strings.EqualFold(e.Code, "invalid_partition") || strings.EqualFold(e.Code, "partition_not_found") || strings.EqualFold(e.Code, "PARTITION_NOT_FOUND")
	case ErrNotLeader:
		return e.Code == "NOT_LEADER"
	default:
		return false
	}
}

func brokerErrorFromWire(remote *wire.BrokerError) *BrokerError {
	if remote == nil {
		return nil
	}
	fields := make(map[string]string, len(remote.Fields))
	for key, value := range remote.Fields {
		fields[key] = value
	}
	return &BrokerError{
		Code:      remote.Code,
		Class:     wireprotocol.ErrorClass(remote.Class.String()),
		Retryable: remote.Retryable,
		Message:   remote.Message,
		Fields:    fields,
	}
}

// ParseBrokerError converts the text-protocol error envelope into the same
// structured error used by the framed transport. It accepts both `ERROR:` and
// `ERROR ` prefixes because older brokers emit both forms.
func ParseBrokerError(value string) (*BrokerError, bool) {
	value = strings.TrimSpace(value)
	upper := strings.ToUpper(value)
	if strings.HasPrefix(upper, "ERROR:") {
		value = strings.TrimSpace(value[len("ERROR:"):])
	} else if strings.HasPrefix(upper, "ERROR ") {
		value = strings.TrimSpace(value[len("ERROR "):])
	} else {
		return nil, false
	}
	parsed, ok := wireprotocol.ParseErrorResponse("ERROR: " + value)
	if !ok {
		return nil, false
	}
	message := parsed.Fields["reason"]
	if message == "" {
		message = parsed.Code
	}
	return &BrokerError{Code: parsed.Code, Class: parsed.Class, Retryable: parsed.Retryable, Message: message, Fields: parsed.Fields}, true
}
