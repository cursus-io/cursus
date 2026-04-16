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
	if value.Tag == "!!null" {
		return fmt.Errorf("log_level cannot be null")
	}

	switch value.Kind {
	case yaml.ScalarNode:
		switch value.Tag {
		case "!!str":
			*l = parseLogLevelString(value.Value)
			return nil
		case "!!int":
			var i int
			if err := value.Decode(&i); err != nil {
				return err
			}
			if i < 0 || i > int(LogLevelError) {
				return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
			}
			*l = LogLevel(i)
			return nil
		default:
			return fmt.Errorf("log_level must be a string or integer")
		}
	default:
		return fmt.Errorf("log_level must be a string or integer")
	}
}

// UnmarshalJSON implements custom JSON unmarshaling for LogLevel
func (l *LogLevel) UnmarshalJSON(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	switch val := v.(type) {
	case string:
		*l = parseLogLevelString(val)
		return nil
	case float64:
		i := int(val)
		if float64(i) != val {
			return fmt.Errorf("log_level integer must be a whole number")
		}
		if i < 0 || i > int(LogLevelError) {
			return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
		}
		*l = LogLevel(i)
		return nil
	case nil:
		return fmt.Errorf("log_level cannot be null")
	default:
		return fmt.Errorf("log_level must be a string or integer")
	}
}
