package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeDefinitionPatchTreatsNilAndEmptyACLAsEquivalent(t *testing.T) {
	current := DefaultDefinition("orders", nil)
	emptyACL := []string{}

	merged, err := MergeDefinitionPatch(current, DefinitionPatch{ReadACL: &emptyACL}, true)
	require.NoError(t, err)
	require.Equal(t, current.Revision, merged.Revision)
	require.Empty(t, merged.Policy.ReadACL)
}
