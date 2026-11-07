package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestOffsetSnapshotLostReplyDoesNotReuseRevision replays changed writes after a lost reply without revision collisions or premature offset visibility.
func TestOffsetSnapshotLostReplyDoesNotReuseRevision(t *testing.T) {
	for _, mode := range []string{"single", "bulk", "cooperative", "multi-topic", "transaction"} {
		for _, replace := range []bool{false, true} {
			name := mode
			if replace {
				name += "/state-replacement"
			}
			t.Run(name, func(t *testing.T) {
				c, parts, records := runtimeCooperativeFixture(t, 2)
				var commit func(uint64) error
				switch mode {
				case "single":
					commit = func(offset uint64) error { return c.CommitOffset("workers", "orders", 0, offset) }
				case "bulk":
					commit = func(offset uint64) error {
						return c.CommitOffsetsBulk("workers", "orders", []OffsetItem{{Partition: 0, Offset: offset}})
					}
				case "cooperative":
					member := runtimeJoin(t, c, parts, "member")
					commit = func(offset uint64) error {
						return c.CommitCooperative("workers", member.RegistrationEpoch, member.Grants[0], offset)
					}
				case "multi-topic":
					_, err := c.AddConsumer("workers", "member")
					require.NoError(t, err)
					generation := c.GetGroup("workers").Generation
					commit = func(offset uint64) error {
						return c.ValidateAndCommitTopicOffsetsBulk("workers", "member", generation, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: offset}}})
					}
				case "transaction":
					epoch := c.GetGroup("workers").RegistrationEpoch
					commit = func(offset uint64) error {
						return c.MaterializeCommittedTransactionOffsets("workers", epoch, map[string][]OffsetItem{"orders": {{Partition: 0, Offset: offset}}})
					}
				}
				require.NoError(t, commit(8))
				before := c.ExportState()
				lost := true
				c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
					*records = append(*records, record)
					if lost {
						lost = false
						return errors.New("reply lost after durable append")
					}
					return nil
				})
				require.ErrorContains(t, commit(16), "reply lost")
				require.Equal(t, uint64(8), mustOffset(t, c, "workers", "orders", 0))
				require.Equal(t, uint64(1), c.ExportState()["workers"].OffsetRevisions["orders"], "failed writes must not masquerade as acknowledged state")
				if replace {
					require.NoError(t, c.importStateValidated(before))
				}
				require.NoError(t, commit(24))
				require.Equal(t, uint64(3), c.ExportState()["workers"].OffsetRevisions["orders"])
				var messages []types.Message
				bytes := 0
				for i, record := range *records {
					message := encodedMetadataMessage(t, record, uint64(i))
					bytes += len(message.Payload)
					messages = append(messages, message)
				}
				reader := &metadataReplayHandler{messages: map[int][]types.Message{0: messages}}
				recovered, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), reader)
				require.NoError(t, err)
				t.Cleanup(recovered.Stop)
				_, err = recovered.recoverConsumerMetadataWithOptions(reader, true)
				require.NoError(t, err, "strict replay must still reject actual conflicting revisions")
				require.Equal(t, uint64(24), mustOffset(t, recovered, "workers", "orders", 0))
				t.Logf("replay_records=%d replay_payload_bytes=%d", len(messages), bytes)
			})
		}
	}
}

// TestOffsetRevisionOverflowAndNewRegistration rejects exhausted revisions without appending and resets attempt history for a new epoch.
func TestOffsetRevisionOverflowAndNewRegistration(t *testing.T) {
	c, _, records := runtimeCooperativeFixture(t, 1)
	group := c.GetGroup("workers")
	group.OffsetRevisions["orders"] = ^uint64(0)
	writes := len(*records)
	require.ErrorContains(t, c.CommitOffset("workers", "orders", 0, 8), "offset revision overflow")
	require.Len(t, *records, writes)
	group.OffsetRevisions["orders"] = 0
	group.offsetWriteRevisions = map[string]uint64{"orders": ^uint64(0)}
	require.ErrorContains(t, c.CommitOffsetsBulk("workers", "orders", []OffsetItem{{Partition: 0, Offset: 8}}), "offset revision overflow")
	require.Len(t, *records, writes)
	next := c.ExportState()
	next["workers"].RegistrationEpoch++
	require.NoError(t, c.importStateValidated(next))
	require.NoError(t, c.CommitOffset("workers", "orders", 0, 8))
	require.Equal(t, uint64(1), (*records)[len(*records)-1].Revision)
}

// TestOffsetAttemptRevisionSurvivesStrictAndLegacyReplay keeps uncertain attempt floors across in-process replay while retaining acknowledged offsets.
func TestOffsetAttemptRevisionSurvivesStrictAndLegacyReplay(t *testing.T) {
	for _, strict := range []bool{false, true} {
		name := "legacy"
		if strict {
			name = "strict"
		}
		t.Run(name, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
			reader := strictReader(c)
			var last ConsumerMetadataRecord
			lost := false
			c.SetRoutedOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
				last = record
				c.RecordConsumerMetadataSource(record.Group, 0)
				if lost {
					return errors.New("uncertain append")
				}
				reader.messages[0] = append(reader.messages[0], encodedMetadataMessage(t, record, uint64(len(reader.messages[0]))))
				return nil
			})
			require.NoError(t, c.CommitOffset("g", "events", 0, 8))
			lost = true
			require.ErrorContains(t, c.CommitOffset("g", "events", 0, 16), "uncertain append")
			if strict {
				release, err := c.BeginConsumerMetadataRecovery(context.Background())
				require.NoError(t, err)
				prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
				require.NoError(t, err)
				require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
				release()
			} else {
				_, err := c.recoverConsumerMetadata(reader)
				require.NoError(t, err)
			}
			require.Equal(t, uint64(8), mustOffset(t, c, "g", "events", 0))
			lost = false
			require.NoError(t, c.CommitOffset("g", "events", 0, 24))
			require.Equal(t, uint64(3), last.Revision)
		})
	}
}
