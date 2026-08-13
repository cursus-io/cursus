package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"

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

func TestRunConsumerMetadataInspectIsReadOnly(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{"consumer-metadata", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.JSONEq(t, `{"records":[]}`, stdout.String())
	require.Empty(t, stderr.String())
	require.NoFileExists(t, filepath.Join(root, config.ConsumerMetadataMigrationFileName))
}

func TestRunConsumerMetadataMigrationDryRunDoesNotWrite(t *testing.T) {
	root := t.TempDir()
	selection := filepath.Join(t.TempDir(), "selection.json")
	require.NoError(t, os.WriteFile(selection, []byte(`{"version":1,"groups":[]}`), 0o600))
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, run([]string{
		"consumer-metadata", "migrate", "--log-dir", root, "--selection", selection, "--dry-run",
	}, &stdout, &stderr))
	require.Contains(t, stdout.String(), `"inventory_sha256"`)
	require.Empty(t, stderr.String())
	require.NoFileExists(t, filepath.Join(root, config.ConsumerMetadataMigrationFileName))
}
func TestRunOrphanInspectRequiresManifest(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"orphan", "inspect", "--log-dir", root}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "no topic metadata manifest is present")
}
