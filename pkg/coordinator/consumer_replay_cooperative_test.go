package coordinator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func appendCooperativeIndexFixture(t *testing.T, r *replayIndexTestReader) ConsumerMetadataRecord {
	t.Helper()
	registration := ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration, Group: "workers", Topic: "orders", PartitionCount: 6, Epoch: 19, Timestamp: time.Unix(90, 0).UTC()}
	record := cooperativeLifecycleRecord(t)
	for _, item := range []ConsumerMetadataRecord{registration, record} {
		r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, item, uint64(len(r.messages[0]))))
	}
	return record
}

func TestConsumerReplayIndexCooperativeOwnershipAndClocks(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			record := appendCooperativeIndexFixture(t, r)
			other := ConsumerMetadataRecord{Version: 4, Type: ConsumerMetadataRecordLifecycleSnapshot, Group: "g", Epoch: 1, Revision: 1, Lifecycle: &GroupLifecycleSnapshot{TopicName: "events", Generation: 1, Partitions: []int{0}, Members: []GroupLifecycleMember{{ID: "m", Assignments: []int{0}}}}}
			r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, other, uint64(len(r.messages[0]))))
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.Empty(t, p.status.IndexFailure)
			require.Positive(t, p.status.IndexWriteBytes)
			require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
			clock := time.Now().Add(-time.Minute)
			for _, name := range []string{"g", "workers"} {
				group := c.groups[name]
				group.LastActivity = clock
				c.ownershipSince[name] = clock
				for _, member := range group.Members {
					member.LastHeartbeat = clock
				}
			}
			invalidate := func() {
				path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
				switch mode {
				case "missing":
					require.NoError(t, os.Remove(path))
				case "corrupt":
					require.NoError(t, os.WriteFile(path, []byte("torn"), 0600))
				}
			}
			replay := func() {
				invalidate()
				p, err = c.PrepareFullConsumerMetadataReplay(r)
				require.NoError(t, err)
				require.Equal(t, mode == "valid", p.status.IndexHit)
				require.Empty(t, p.status.IndexFailure)
				require.NoError(t, c.PreserveConsumerReplayClocks(p, func(string) (int, error) { return 0, nil }, map[int]bool{0: true}))
				require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
			}
			replay()
			require.Equal(t, record.Lifecycle.Cooperative, c.groups["workers"].Cooperative)
			require.Len(t, cooperativeAcks(c.groups["workers"].Cooperative, "a"), 3)
			require.Equal(t, clock, c.groups["workers"].Members["a"].LastHeartbeat)
			require.Equal(t, clock, c.ownershipSince["workers"])

			// A new durable grant epoch with the same eager generation and
			// assignment must not inherit the replaced ownership's clocks.
			record = canonicalConsumerMetadataRecord(record)
			record.Revision++
			record.Lifecycle.Cooperative.Revision = record.Revision
			for i := range record.Lifecycle.Cooperative.Current {
				grant := &record.Lifecycle.Cooperative.Current[i]
				if grant.RevokeRevision == 0 {
					grant.Epoch = record.Revision
					break
				}
			}
			require.NoError(t, validateConsumerMetadataRecord(record))
			r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, uint64(len(r.messages[0]))))
			replay()
			require.Equal(t, record.Lifecycle.Cooperative, c.groups["workers"].Cooperative)
			require.NotEqual(t, clock, c.groups["workers"].Members["a"].LastHeartbeat)
			require.NotContains(t, c.ownershipSince, "workers")
			require.Equal(t, clock, c.groups["g"].Members["m"].LastHeartbeat)
			require.Equal(t, clock, c.groups["g"].LastActivity)
			require.Equal(t, clock, c.ownershipSince["g"])
			require.Equal(t, uint64(10), c.groups["g"].Offsets["events"][0])
		})
	}
}
