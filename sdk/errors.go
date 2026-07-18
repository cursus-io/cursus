package sdk

import (
	"errors"
	"strings"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

var (
	ErrProducerClosed   = errors.New("producer closed")
	ErrConsumerClosed   = errors.New("consumer closed")
	ErrTopicNotFound    = errors.New("topic not found")
	ErrInvalidPartition = errors.New("invalid partition")
	ErrNotLeader        = errors.New("not leader")
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
	Fields    map[string]string
	Details   []string
	Raw       string
}

func (e *BrokerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Raw
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

func ParseBrokerError(response string) (*BrokerError, bool) {
	parsed, ok := wireprotocol.ParseErrorResponse(response)
	if !ok {
		return nil, false
	}
	fields := make(map[string]string, len(parsed.Fields))
	for key, value := range parsed.Fields {
		fields[key] = value
	}
	return &BrokerError{
		Code:      parsed.Code,
		Class:     parsed.Class,
		Retryable: parsed.Retryable,
		Fields:    fields,
		Details:   append([]string(nil), parsed.Details...),
		Raw:       parsed.Raw,
	}, true
}
