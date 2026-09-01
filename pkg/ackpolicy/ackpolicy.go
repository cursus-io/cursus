// Package ackpolicy defines the Cursus publish acknowledgement contract shared
// by the broker and SDK.
package ackpolicy

import (
	"fmt"
	"strings"
)

type Mode string

const (
	None   Mode = "0"
	Leader Mode = "1"
	All    Mode = "all"
)

type Selection struct {
	Requested string
	Mode      Mode
}

// Parse validates an acknowledgement value while preserving the -1 wire alias.
// An empty value retains the Cursus default of leader acknowledgement.
func Parse(value string) (Selection, error) {
	requested := strings.ToLower(strings.TrimSpace(value))
	if requested == "" {
		requested = "1"
	}
	switch requested {
	case "0":
		return Selection{Requested: requested, Mode: None}, nil
	case "1":
		return Selection{Requested: requested, Mode: Leader}, nil
	case "all", "-1":
		return Selection{Requested: requested, Mode: All}, nil
	default:
		return Selection{}, fmt.Errorf("invalid acknowledgement mode %q", value)
	}
}

func (s Selection) SupportsIdempotence() bool {
	return s.Mode == All
}
