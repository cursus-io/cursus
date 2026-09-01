package wire

import (
	"fmt"
	"sort"
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

type ErrorPayload struct {
	Code      string
	Class     ErrorClass
	Retryable bool
	Message   string
	Fields    map[string]string
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
