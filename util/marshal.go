package util

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// IsNil checks if the interface value is nil, including its dynamic value.
func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return v.IsNil()
	}
	return false
}

type LogLevel int

const (
	LogLevelDebug LogLevel = 0
	LogLevelInfo  LogLevel = 1
	LogLevelWarn  LogLevel = 2
	LogLevelError LogLevel = 3
)

func parseLogLevelString(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug", "0":
		return LogLevelDebug
	case "info", "1":
		return LogLevelInfo
	case "warn", "warning", "2":
		return LogLevelWarn
	case "error", "3":
		return LogLevelError
	default:
		return LogLevelInfo
	}
}

// UnmarshalYAML implements custom YAML unmarshaling for LogLevel
func (l *LogLevel) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		*l = parseLogLevelString(s)
		return nil
	}

	var i int
	if err := value.Decode(&i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	if i < 0 || i > int(LogLevelError) {
		return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
	}
	*l = LogLevel(i)
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for LogLevel
func (l *LogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*l = parseLogLevelString(s)
		return nil
	}

	var i int
	if err := json.Unmarshal(data, &i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	if i < 0 || i > int(LogLevelError) {
		return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
	}
	*l = LogLevel(i)
	return nil
}
