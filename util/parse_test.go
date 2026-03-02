package util_test

import (
	"testing"

	"github.com/cursus-io/cursus/util"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		fallback int
		want     int
	}{
		{"123", 0, 123},
		{"0", 99, 0},
		{"-5", 0, -5},
		{"abc", 42, 42},
		{"", 7, 7},
		{"   ", 8, 8},
	}

	for _, tt := range tests {
		got := util.ParseInt(tt.input, tt.fallback)
		if got != tt.want {
			t.Errorf("ParseInt(%q, %d) = %d; want %d", tt.input, tt.fallback, got, tt.want)
		}
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input    string
		fallback bool
		want     bool
	}{
		{"true", false, true},
		{"false", true, false},
		{"1", false, true},
		{"0", true, false},
		{"t", false, true},
		{"f", true, false},
		{"yes", false, false},
		{"", true, true},
		{"   ", false, false},
	}

	for _, tt := range tests {
		got := util.ParseBool(tt.input, tt.fallback)
		if got != tt.want {
			t.Errorf("ParseBool(%q, %v) = %v; want %v", tt.input, tt.fallback, got, tt.want)
		}
	}
}
