package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeDefinitionPatchAggregateReplayEnablesPrerequisites(t *testing.T) {
	profile := true
	definition, err := MergeDefinitionPatch(DefaultDefinition("matches", nil), DefinitionPatch{AggregateReplay: &profile}, false)
	require.NoError(t, err)
	require.True(t, definition.Policy.AggregateReplay)
	require.True(t, definition.EventSourcing)
	require.True(t, definition.Idempotent)
}

func TestMergeDefinitionPatchTreatsNilAndEmptyACLAsEquivalent(t *testing.T) {
	current := DefaultDefinition("orders", nil)
	emptyACL := []string{}

	merged, err := MergeDefinitionPatch(current, DefinitionPatch{ReadACL: &emptyACL}, true)
	require.NoError(t, err)
	require.Equal(t, current.Revision, merged.Revision)
	require.Empty(t, merged.Policy.ReadACL)
}
