package ackpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNormalizesAcknowledgementModes(t *testing.T) {
	tests := []struct {
		input     string
		requested string
		mode      Mode
	}{
		{input: "", requested: "1", mode: Leader},
		{input: " 0 ", requested: "0", mode: None},
		{input: "1", requested: "1", mode: Leader},
		{input: "ALL", requested: "all", mode: All},
		{input: "-1", requested: "-1", mode: All},
	}
	for _, test := range tests {
		selection, err := Parse(test.input)
		require.NoError(t, err)
		require.Equal(t, test.requested, selection.Requested)
		require.Equal(t, test.mode, selection.Mode)
	}
	for _, input := range []string{"2", "leader", "-2"} {
		_, err := Parse(input)
		require.Error(t, err)
	}
}
