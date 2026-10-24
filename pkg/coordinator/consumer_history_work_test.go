package coordinator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryWorkDetachedAdmissionAndSuffix(t *testing.T) {
	_, reader := newIndexFixture(t, 100)
	c := newAuthorityTestCoordinator(t)
	work, err := c.TryCaptureConsumerHistoryWork()
	require.NoError(t, err)
	require.NoError(t, c.ConsumerOperationsReadinessError())
	reader.onRaw = func() {
		require.NoError(t, c.ConsumerOperationsReadinessError(), "certification must not own live admission")
	}
	plan, err := work.Prepare(reader, 0, 0, 101)
	require.NoError(t, err)
	require.Len(t, plan.removed, 99)
	require.Error(t, c.ValidateConsumerHistoryCompaction(reader, plan), "installation requires a fresh live lease")
	acquireReplay(t, c)
	reader.messages[0] = append(reader.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 101, 101), 101))
	require.NoError(t, c.ValidateConsumerHistoryCompaction(reader, plan), "an ordinary suffix does not invalidate the certified prefix")
	reader.identity = "replaced"
	require.Error(t, c.ValidateConsumerHistoryCompaction(reader, plan))
}

func TestConsumerHistoryCaptureDoesNotCloseAdmission(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	before := c.ConsumerRecoveryRequirement()
	c.authorityMu.Lock()
	_, err := c.TryCaptureConsumerHistoryWork()
	c.authorityMu.Unlock()
	require.ErrorIs(t, err, ErrConsumerSnapshotBusy)
	require.Equal(t, before, c.ConsumerRecoveryRequirement())
	require.NoError(t, c.ConsumerOperationsReadinessError())
	work, err := c.TryCaptureConsumerHistoryWork()
	require.NoError(t, err)
	require.NotNil(t, work)
	require.Equal(t, before, c.ConsumerRecoveryRequirement())
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestConsumerHistoryWorkRejectsChangedCompatibility(t *testing.T) {
	_, reader := newIndexFixture(t, 10)
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	work, err := c.CaptureConsumerHistoryWork()
	require.NoError(t, err)
	release()
	plan, err := work.Prepare(reader, 0, 0, 11)
	require.NoError(t, err)
	require.NoError(t, c.RegisterGroupFromFSM("events", "unrelated", 1))
	acquireReplay(t, c)
	require.ErrorContains(t, c.ValidateConsumerHistoryCompaction(reader, plan), "authority changed")
}
