package wire

import (
	"fmt"
	"sort"
	"strings"
)

type ErrorClass uint8

const (
	ErrorClassValidation ErrorClass = iota + 1
	ErrorClassAuthorization
	ErrorClassRouting
	ErrorClassAvailability
	ErrorClassConflict
	ErrorClassFencing
	ErrorClassNotFound
	ErrorClassInternal
)

func ParseErrorClass(value string) (ErrorClass, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "validation":
		return ErrorClassValidation, nil
	case "authorization":
		return ErrorClassAuthorization, nil
	case "routing":
		return ErrorClassRouting, nil
	case "availability":
		return ErrorClassAvailability, nil
	case "conflict":
		return ErrorClassConflict, nil
	case "fencing":
		return ErrorClassFencing, nil
	case "not_found":
		return ErrorClassNotFound, nil
	case "internal":
		return ErrorClassInternal, nil
	default:
		return 0, fmt.Errorf("unknown Wire v2 error class %q", value)
	}
}

func (c ErrorClass) String() string {
	switch c {
	case ErrorClassValidation:
		return "validation"
	case ErrorClassAuthorization:
		return "authorization"
	case ErrorClassRouting:
		return "routing"
	case ErrorClassAvailability:
		return "availability"
	case ErrorClassConflict:
		return "conflict"
	case ErrorClassFencing:
		return "fencing"
	case ErrorClassNotFound:
		return "not_found"
	case ErrorClassInternal:
		return "internal"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

type ErrorPayload struct {
	Code      string
	Class     ErrorClass
	Retryable bool
	Message   string
	Fields    map[string]string
}

// BrokerError is the typed client-side representation of a Wire v2 error
// response. Error metadata remains structured all the way through the client
// transport; callers must not recover it by parsing Error().
type BrokerError struct {
	Code      string
	Class     ErrorClass
	Retryable bool
	Message   string
	Fields    map[string]string
}

func NewBrokerError(payload ErrorPayload) *BrokerError {
	fields := make(map[string]string, len(payload.Fields))
	for key, value := range payload.Fields {
		fields[key] = value
	}
	return &BrokerError{
		Code:      payload.Code,
		Class:     payload.Class,
		Retryable: payload.Retryable,
		Message:   payload.Message,
		Fields:    fields,
	}
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

func EncodeError(payload ErrorPayload) ([]byte, error) {
	if payload.Code == "" || payload.Class < ErrorClassValidation || payload.Class > ErrorClassInternal {
		return nil, fmt.Errorf("invalid Wire v2 error code=%q class=%d", payload.Code, payload.Class)
	}
	if len(payload.Fields) > 256 {
		return nil, fmt.Errorf("error field count %d exceeds maximum 256", len(payload.Fields))
	}
	encoder := newBinaryEncoder(MaxFramePayload)
	encoder.string(payload.Code)
	class := encoder.append(1)
	if len(class) == 1 {
		class[0] = byte(payload.Class)
	}
	retryable := encoder.append(1)
	if len(retryable) == 1 && payload.Retryable {
		retryable[0] = 1
	}
	encoder.string(payload.Message)
	// #nosec G115 -- the field count is checked against the 256-field protocol limit above.
	encoder.uint16(uint16(len(payload.Fields)))
	keys := make([]string, 0, len(payload.Fields))
	for key := range payload.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("error field name is empty")
		}
		encoder.string(key)
		encoder.string(payload.Fields[key])
	}
	return encoder.result()
}

func DecodeError(data []byte) (ErrorPayload, error) {
	decoder := newBinaryDecoder(data)
	payload := ErrorPayload{Code: decoder.string()}
	class := decoder.take(1)
	if len(class) == 1 {
		payload.Class = ErrorClass(class[0])
	}
	retryable := decoder.take(1)
	if len(retryable) == 1 {
		if retryable[0] > 1 {
			return ErrorPayload{}, fmt.Errorf("invalid retryable flag %d", retryable[0])
		}
		payload.Retryable = retryable[0] == 1
	}
	payload.Message = decoder.string()
	count := decoder.uint16()
	payload.Fields = make(map[string]string, count)
	for range count {
		key := decoder.string()
		value := decoder.string()
		if key == "" {
			return ErrorPayload{}, fmt.Errorf("error field name is empty")
		}
		if _, exists := payload.Fields[key]; exists {
			return ErrorPayload{}, fmt.Errorf("duplicate error field %q", key)
		}
		payload.Fields[key] = value
	}
	if err := decoder.finish(); err != nil {
		return ErrorPayload{}, err
	}
	if payload.Code == "" || payload.Class < ErrorClassValidation || payload.Class > ErrorClassInternal {
		return ErrorPayload{}, fmt.Errorf("invalid Wire v2 error code=%q class=%d", payload.Code, payload.Class)
	}
	return payload, nil
}
