package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryCompactionRetainsCooperativeFences(t *testing.T) {
	c, r := newIndexFixture(t, 100)
	c.cfg.ConsumerRecoveryIndexEnabled = false
	record := appendCooperativeIndexFixture(t, r)
	for range 100 {
		// Reusing a member name must advance its session and the pending
		// revocation fence even when the eager assignment stays unchanged.
		s := record.Lifecycle.Cooperative
		var err error
		s, err = s.Remove([]CooperativeMember{{ID: "b", Session: s.session("b")}}, nil)
		require.NoError(t, err)
		s, err = s.Join("b", nil)
		require.NoError(t, err)
		record.Lifecycle.Cooperative = s
		record.Revision = s.Revision
		r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, uint64(len(r.messages[0]))))
	}
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	expected := p.scratch.ExportState()
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	clock := time.Now().Add(-time.Minute)
	c.groups["workers"].Members["a"].LastHeartbeat = clock
	c.ownershipSince["workers"] = clock
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, uint64(len(r.messages[0])))
	require.NoError(t, err)
	require.Len(t, plan.RemovedRecords(), 199)
	require.Equal(t, expected, c.ExportState(), "certification cannot install recovered state")
	require.Equal(t, clock, c.groups["workers"].Members["a"].LastHeartbeat)
	require.Equal(t, clock, c.ownershipSince["workers"])
	applyHistoryPlanToFixture(r, plan)
	require.Len(t, r.messages[0], 4)
	r.identity = "compacted-durable-generation"
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, p.status.IndexHit)
	require.Equal(t, expected, p.scratch.ExportState())
	require.Equal(t, record.Lifecycle.Cooperative, p.scratch.groups["workers"].Cooperative)
	require.Len(t, cooperativeAcks(p.scratch.groups["workers"].Cooperative, "a"), 3)

	// Erasing old lifecycle records must not permit an eager downgrade of
	// the same registration after cache-free recovery of the retained log.
	eager := canonicalConsumerMetadataRecord(record)
	eager.Version, eager.Revision, eager.Lifecycle.Cooperative = 4, 0, nil
	next := r.messages[0][len(r.messages[0])-1].Offset + 1
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, eager, next))
	_, err = c.PrepareFullConsumerMetadataReplay(r)
	require.ErrorContains(t, err, "eager lifecycle follows cooperative")
}
