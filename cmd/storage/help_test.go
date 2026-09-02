package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunHelpReturnsSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"--help"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "manifest inspect")
	require.Contains(t, stdout.String(), "consumer-metadata inspect")
	require.NotContains(t, stdout.String(), "manifest create")
	require.NotContains(t, stdout.String(), "consumer-metadata migrate")
	require.Empty(t, stderr.String())
}
