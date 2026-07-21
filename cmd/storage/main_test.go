package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRejectsIncompleteCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run([]string{"manifest"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "usage:")
}

func TestRunManifestInspectWritesJSON(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"manifest", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.JSONEq(t, `{"manifest_present":false,"topics":[]}`, stdout.String())
	require.Empty(t, stderr.String())
}

func TestRunManifestCreateRejectsInvalidDefinitions(t *testing.T) {
	root := t.TempDir()
	definitions := filepath.Join(t.TempDir(), "definitions.json")
	require.NoError(t, os.WriteFile(definitions, []byte(`{"version":1,"topics":[],"unknown":true}`), 0o600))
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"manifest", "create", "--log-dir", root, "--definitions", definitions}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "unknown field")
}

func TestRunOrphanInspectRequiresManifest(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"orphan", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "no topic metadata manifest is present")
}
