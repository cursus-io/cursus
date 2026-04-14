package util

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestIsNil(t *testing.T) {
	assert.True(t, IsNil(nil))
	
	var p *int
	assert.True(t, IsNil(p))
	
	var s []int
	assert.True(t, IsNil(s))
	
	var m map[string]int
	assert.True(t, IsNil(m))
	
	var c chan int
	assert.True(t, IsNil(c))
	
	var i interface{}
	assert.True(t, IsNil(i))
	
	assert.False(t, IsNil(10))
	assert.False(t, IsNil("hello"))
}

func TestLogLevel_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
		wantErr  bool
	}{
		{`"debug"`, LogLevelDebug, false},
		{`"INFO"`, LogLevelInfo, false},
		{`"warn"`, LogLevelWarn, false},
		{`"error"`, LogLevelError, false},
		{`"unknown"`, LogLevelInfo, false}, // defaults to info
		{`0`, LogLevelDebug, false},
		{`1`, LogLevelInfo, false},
		{`2`, LogLevelWarn, false},
		{`3`, LogLevelError, false},
		{`4`, 0, true},
		{`-1`, 0, true},
		{`true`, 0, true},
	}

	for _, tt := range tests {
		var l LogLevel
		err := json.Unmarshal([]byte(tt.input), &l)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, l)
		}
	}
}

func TestLogLevel_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
		wantErr  bool
	}{
		{"debug", LogLevelDebug, false},
		{"INFO", LogLevelInfo, false},
		{"warning", LogLevelWarn, false},
		{"error", LogLevelError, false},
		{"0", LogLevelDebug, false},
		{"1", LogLevelInfo, false},
		{"2", LogLevelWarn, false},
		{"3", LogLevelError, false},
	}

	for _, tt := range tests {
		var l LogLevel
		err := yaml.Unmarshal([]byte(tt.input), &l)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, l)
		}
	}
}
