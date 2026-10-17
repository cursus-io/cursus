package controller

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRetainedRecoveryBindsEveryResponseToOriginalChallenge(t *testing.T) {
	prefix := disk.RetainedPrefix{Version: 1, Start: 0, End: 2, Digest: strings.Repeat("a", 64)}
	r := fsm.ReplicaCatchupRequest{NextOffset: 2, CommittedHWM: 3, MaxRecords: 1, RecoveryProtocol: 2, RetainedPrefix: &prefix}
	valid := fsm.ReplicaCatchupBatch{StartOffset: 2, EndOffset: 3, CommittedHWM: 3, RecoveryProtocol: 2,
		RetainedPrefix: &prefix, MatchedRetainedPrefix: &prefix, Verified: true, Messages: []types.Message{{Offset: 2}}}
	for _, variant := range []string{"normal", "bootstrap", "bootstrap changed challenge", "changed match", "legacy prefix", "rollback", "missing verification", "downgrade", "missing challenge", "bootstrap with match", "checksum"} {
		t.Run(variant, func(t *testing.T) {
			b := valid
			switch variant {
			case "bootstrap", "bootstrap changed challenge", "bootstrap with match":
				b.BootstrapRequired, b.Verified, b.EndOffset, b.Messages = true, false, 2, nil
				if variant != "bootstrap with match" {
					b.MatchedRetainedPrefix = nil
				}
				if variant == "bootstrap changed challenge" {
					p := prefix
					p.Digest = strings.Repeat("b", 64)
					b.RetainedPrefix = &p
				}
			case "changed match":
				p := prefix
				p.End = 1
				b.MatchedRetainedPrefix = &p
			case "legacy prefix":
				b.MatchedPrefix = &disk.LineagePrefix{}
			case "rollback":
				to := uint64(1)
				b.TruncateTo = &to
			case "missing verification":
				b.Verified = false
			case "downgrade":
				b.RecoveryProtocol = 1
			case "missing challenge":
				b.RetainedPrefix = nil
			}
			sealed, err := fsm.SealReplicaCatchupBatch(b)
			require.NoError(t, err)
			if variant == "checksum" {
				p := prefix
				p.Digest = strings.Repeat("b", 64)
				sealed.RetainedPrefix = &p
			}
			err = ValidateReplicaCatchupBatch(r, sealed)
			switch variant {
			case "normal":
				require.NoError(t, err)
			case "bootstrap":
				require.ErrorIs(t, err, fsm.ErrReplicaBootstrapRequired)
			default:
				require.Error(t, err)
				require.NotErrorIs(t, err, fsm.ErrReplicaBootstrapRequired, "invalid responses must not trigger an install")
			}
		})
	}
}

func TestRetainedRecoveryAdvanceDiscardsWireDerivedProof(t *testing.T) {
	prefix := disk.RetainedPrefix{Version: 1, End: 2, Digest: strings.Repeat("a", 64)}
	r := fsm.ReplicaCatchupRequest{NextOffset: 2, RecoveryProtocol: 2, RetainedPrefix: &prefix}
	require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&r, fsm.ReplicaCatchupBatch{EndOffset: 4, Messages: []types.Message{{Offset: 3, Payload: "last"}}}))
	require.Equal(t, uint64(4), r.NextOffset)
	require.Nil(t, r.RetainedPrefix)
	require.Nil(t, r.LocalWitness)
	require.Empty(t, r.PreviousRecordDigest, "refresh from durable local storage, not remote messages")
}
