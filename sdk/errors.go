package sdk

import (
	"errors"
	"fmt"
	"strings"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

var (
	ErrProducerClosed      = errors.New("producer closed")
	ErrConsumerClosed      = errors.New("consumer closed")
	ErrConsumerRebalancing = errors.New("consumer assignment is rebalancing")
	ErrTopicNotFound       = errors.New("topic not found")
	ErrInvalidPartition    = errors.New("invalid partition")
	ErrNotLeader           = errors.New("not leader")
)

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
	if e.Message == "" {
		return fmt.Sprintf("broker error %s (%s)", e.Code, e.Class)
	}
	return fmt.Sprintf("broker error %s (%s): %s", e.Code, e.Class, e.Message)
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
