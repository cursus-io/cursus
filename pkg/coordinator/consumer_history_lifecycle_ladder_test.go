package coordinator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reusing the same four names must not retain every past registration epoch.
// Deleted-name fences remain authoritative even when their old records leave.
func TestConsumerHistoryLifecycleLadder(t *testing.T) {
	for _, history := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			c, r := newIndexFixture(t, 0)
			c.cfg.ConsumerRecoveryIndexEnabled = false
			r.messages[0] = nil
			appendRecord := func(record ConsumerMetadataRecord) {
				r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, uint64(len(r.messages[0]))))
			}
			for revision := 1; revision <= history; revision++ {
				for group := 0; group < 4; group++ {
					name := fmt.Sprintf("fixed-%d", group)
					epoch := uint64(2 * revision)
					appendRecord(ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: name, Epoch: epoch - 1})
					appendRecord(replayRegistration(name, epoch))
					appendRecord(replayOffset(name, epoch, 1, uint64(revision)))
				}
			}
			appendRecord(replayRegistration("deleted", 1))
			appendRecord(ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "deleted", Epoch: 2})
			end := uint64(len(r.messages[0]))
			before, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, end)
			require.NoError(t, err)
			applyHistoryPlanToFixture(r, plan)
			after, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.Equal(t, before.scratch.ExportState(), after.scratch.ExportState())
			require.Len(t, after.scratch.groups, 4)
			require.Equal(t, uint64(2), after.scratch.groupEpochs["deleted"])
			for group := 0; group < 4; group++ {
				name := fmt.Sprintf("fixed-%d", group)
				require.Equal(t, uint64(2*history), after.scratch.groupEpochs[name])
				require.Equal(t, uint64(history), after.scratch.groups[name].Offsets["events"][0])
			}
			require.LessOrEqual(t, len(r.messages[0]), 13, "retained epoch evidence must be independent of churn")
			t.Logf("lifecycle_cycles=%d input_records=%d retained_records=%d cache_absent_replay_records=%d", history, end, len(r.messages[0]), after.status.ReplayedRecords)
		})
	}
}
