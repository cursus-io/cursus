package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunHelpReturnsSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"--help"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "manifest create")
	require.Empty(t, stderr.String())

	stdout.Reset()
	require.Equal(t, 0, run([]string{"manifest", "create", "--help"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "-definitions")
}
