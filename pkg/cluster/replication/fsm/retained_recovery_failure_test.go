package fsm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRetainedRecoveryIndexedExchangeWithCapablePeers(t *testing.T) {
	for _, variant := range []string{"matched", "truncate", "incomparable", "previous conflict", "previous before zero", "page failure", "lineage failure", "unmarked unsupported", "generation after match", "generation after page"} {
		t.Run(variant, func(t *testing.T) {
			var provider *sourceMutationProvider
			f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}, {Offset: 2, Payload: "three"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &sourceMutationProvider{DiskManager: dm}
				return provider
			})
			retainedRecoveryCapabilities(f)
			r := retainedRequest(t, f, p, 2)
			r.RecoveryProtocol, r.RetainedPrefix = IndexedRecoveryProtocolVersion, nil
			var err error
			r.PrefixCandidates, err = p.ReplicaLineageCandidates(2)
			require.NoError(t, err)
			require.NoError(t, addPreviousReplicaRecord(p, &r))
			failure := errors.New("storage read failed")
			rewrite := func() {
				require.NoError(t, provider.storage.TruncateTo(0))
				require.NoError(t, provider.storage.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "replaced"}, {Topic: "orders", Offset: 1, Payload: "two"}, {Topic: "orders", Offset: 2, Payload: "three"}}))
			}
			switch variant {
			case "truncate":
				for i := range r.PrefixCandidates {
					if r.PrefixCandidates[i].EndOffset == 2 {
						r.PrefixCandidates[i].Digest = strings.Repeat("a", 64)
					}
				}
			case "incomparable":
				r.PrefixCandidates = []disk.LineagePrefix{{StartOffset: 1, EndOffset: 2, Digest: strings.Repeat("a", 64)}}
			case "previous conflict":
				r.PreviousRecordDigest = strings.Repeat("a", 64)
			case "previous before zero":
				r.NextOffset = 0
				r.PrefixCandidates, err = p.ReplicaLineageCandidates(0)
				require.NoError(t, err)
			case "page failure":
				provider.storage.pageErr = failure
			case "lineage failure":
				provider.storage.matchErr = failure
			case "unmarked unsupported":
				provider.storage.matchErr = disk.ErrLineageUnsupported
			case "generation after match":
				provider.storage.afterMatch = rewrite
			case "generation after page":
				r.PreviousRecordDigest = ""
				provider.storage.afterPage = rewrite
			}
			batch, err := f.FetchReplicaCatchup(r)
			// A background checkpoint may briefly own publication. Retry only
			// this explicit transient response, never a lineage/fault result.
			deadline := time.Now().Add(time.Second)
			for errors.Is(err, disk.ErrContentPublicationBusy) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				batch, err = f.FetchReplicaCatchup(r)
			}
			switch variant {
			case "matched":
				require.NoError(t, err)
				require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
				require.NoError(t, ValidateReplicaRecoveryMode(batch))
				require.True(t, batch.Verified)
				require.Equal(t, uint64(3), batch.EndOffset)
				require.Len(t, batch.Messages, 1)
			case "truncate":
				require.NoError(t, err)
				require.NotNil(t, batch.TruncateTo)
				require.Less(t, *batch.TruncateTo, r.NextOffset)
				require.Empty(t, batch.Messages)
				require.NoError(t, ValidateReplicaRecoveryMode(batch))
			case "incomparable":
				require.NoError(t, err)
				require.ErrorIs(t, ValidateReplicaRecoveryMode(batch), ErrReplicaBootstrapRequired)
				require.Nil(t, batch.TruncateTo)
			default:
				require.Error(t, err)
				require.Empty(t, batch.Digest)
				require.False(t, batch.BootstrapRequired, "storage failures must not authorize replacement")
				if variant == "page failure" || variant == "lineage failure" {
					require.ErrorIs(t, err, failure)
				}
			}
			require.Equal(t, uint64(3), p.NextOffset(), "source catch-up must not mutate the partition tail")
		})
	}
}

func TestRetainedRecoveryRejectsInvalidResponseAuthority(t *testing.T) {
	f, p := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: "one"}})
	retainedRecoveryCapabilities(f)
	r := retainedRequest(t, f, p, 0)
	valid, err := f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	for _, variant := range []string{"legacy", "indexed", "old proof", "truncate", "missing challenge", "missing match", "changed match", "unverified end", "bootstrap match", "bootstrap messages", "bootstrap end", "bootstrap compacted", "bootstrap verified"} {
		t.Run(variant, func(t *testing.T) {
			batch := valid
			switch variant {
			case "legacy":
				batch.RecoveryProtocol = 0
			case "indexed":
				batch.RecoveryProtocol = IndexedRecoveryProtocolVersion
			case "old proof":
				batch.MatchedPrefix = &disk.LineagePrefix{}
			case "truncate":
				to := uint64(0)
				batch.TruncateTo = &to
			case "missing challenge":
				batch.RetainedPrefix = nil
			case "missing match":
				batch.MatchedRetainedPrefix = nil
			case "changed match":
				changed := *batch.MatchedRetainedPrefix
				changed.Digest = strings.Repeat("a", 64)
				batch.MatchedRetainedPrefix = &changed
			case "unverified end":
				batch.Verified = false
			default:
				batch.BootstrapRequired = true
				batch.MatchedRetainedPrefix, batch.Messages, batch.EndOffset, batch.Verified = nil, nil, batch.StartOffset, false
				switch variant {
				case "bootstrap match":
					batch.MatchedRetainedPrefix = valid.MatchedRetainedPrefix
				case "bootstrap messages":
					batch.Messages = valid.Messages
				case "bootstrap end":
					batch.EndOffset++
				case "bootstrap compacted":
					batch.Compacted = true
				case "bootstrap verified":
					batch.Verified = true
				}
			}
			err := ValidateReplicaRecoveryMode(batch)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrReplicaBootstrapRequired, "invalid response cannot request installation")
		})
	}
}

func TestRetainedRecoveryJSONRejectsMalformedAndNullProofFields(t *testing.T) {
	for _, raw := range []string{
		`{`,
		`{"recovery_protocol":"2"}`,
		`{"recovery_protocol":2,"retained_prefix":true}`,
		`{"recovery_protocol":2,"retained_prefix":{"version":"1"}}`,
		`{"recovery_protocol":2,"retained_prefix":{"version":1,"start":0,"end":0,"digest":null}}`,
		`{"recovery_protocol":2,"retained_prefix":{"version":1,"start":null,"end":0,"digest":"digest"}}`,
		`{"recovery_protocol":2,"retained_prefix":{"version":1,"start":0,"end":null,"digest":"digest"}}`,
	} {
		var request ReplicaCatchupRequest
		require.Error(t, json.Unmarshal([]byte(raw), &request), raw)
		var batch ReplicaCatchupBatch
		require.Error(t, json.Unmarshal([]byte(raw), &batch), raw)
	}
	f, p := indexedRecoveryFSM(t, nil)
	retainedRecoveryCapabilities(f)
	request := retainedRequest(t, f, p, 0)
	batch, err := f.FetchReplicaCatchup(request)
	require.NoError(t, err)
	raw, err := json.Marshal(batch)
	require.NoError(t, err)
	var decoded ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NoError(t, ValidateReplicaRecoveryMode(decoded))
	require.NoError(t, ValidateReplicaCatchupBatchDigest(decoded))
	require.True(t, decoded.Verified, "explicit zero bounds certify an empty committed log")
}

func TestRetainedRecoveryRejectsUnavailableSourceBeforePublication(t *testing.T) {
	for _, variant := range []string{"invalid request", "missing source field", "missing incarnation", "missing manager", "missing local topic", "changed definition", "missing partition", "behind HWM", "closed storage"} {
		t.Run(variant, func(t *testing.T) {
			var provider *sourceMutationProvider
			f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &sourceMutationProvider{DiskManager: dm}
				return provider
			})
			retainedRecoveryCapabilities(f)
			r := retainedRequest(t, f, p, 0)
			switch variant {
			case "invalid request":
				r.MaxRecords = 0
			case "missing source field":
				r.SourceBroker = ""
			case "missing incarnation":
				f.brokers["node-1"].IncarnationID = ""
			case "missing manager":
				f.tm = nil
			case "missing local topic":
				r.Topic = "absent"
				f.partitionMetadata["absent-0"] = f.partitionMetadata["orders-0"]
				definition := *f.topicState["orders"]
				definition.Name = "absent"
				f.topicState["absent"] = &definition
			case "changed definition":
				f.topicState["orders"].Revision++
			case "missing partition":
				r.Partition = 1
				f.partitionMetadata["orders-1"] = f.partitionMetadata["orders-0"]
			case "behind HWM":
				f.partitionMetadata["orders-0"].CommittedHWM++
			case "closed storage":
				require.NoError(t, provider.storage.Close())
			}
			batch, err := f.FetchReplicaCatchupContext(context.Background(), r)
			if variant == "missing source field" {
				require.NoError(t, err)
				require.Equal(t, "node-1", batch.SourceBroker)
			} else {
				require.Error(t, err)
				require.Empty(t, batch.Digest)
				require.False(t, batch.BootstrapRequired)
			}
		})
	}
}
