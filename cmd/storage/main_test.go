package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRejectsIncompleteCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run([]string{"manifest"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "usage:")
}

func TestRunRejectsRemovedMigrationCommands(t *testing.T) {
	for _, args := range [][]string{
		{"manifest", "create", "--log-dir", t.TempDir()},
		{"consumer-metadata", "migrate", "--log-dir", t.TempDir()},
	} {
		var stdout, stderr bytes.Buffer
		require.Equal(t, 2, run(args, &stdout, &stderr))
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "usage:")
	}
}

func TestRunManifestInspectWritesJSON(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"manifest", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.JSONEq(t, `{"manifest_present":false,"topics":[]}`, stdout.String())
	require.Empty(t, stderr.String())
}

func TestRunConsumerMetadataInspectIsReadOnly(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"consumer-metadata", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.JSONEq(t, `{"records":[]}`, stdout.String())
	require.Empty(t, stderr.String())
}

func TestRunOrphanInspectRequiresManifest(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"orphan", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "no topic metadata manifest is present")
}
