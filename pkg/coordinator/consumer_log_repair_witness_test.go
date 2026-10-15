package coordinator

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConsumerLogRepairCannotReplayWithoutVerifiedLocalRepair(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	defer release()
	generation, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(generation, nil)
	_, err = c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.Error(t, err, "equal log boundaries and a clean rejection cannot discharge a physical repair")
}

type repairReplayReader struct {
	*strictReplayReader
	verified     map[int]uint64
	publishError error
}

func (r *repairReplayReader) ValidateConsumerLogRepair(p int, generation uint64) error {
	if r.verified[p] != generation {
		return fmt.Errorf("repair %d is not verified at generation %d", p, generation)
	}
	return nil
}
func (r *repairReplayReader) AcquireConsumerLogRepairPublication(repairs map[int]uint64) (ConsumerLogRepairPublication, error) {
	for p, g := range repairs {
		if err := r.ValidateConsumerLogRepair(p, g); err != nil {
			return nil, err
		}
	}
	return &repairReplayPublication{r}, nil
}

type repairReplayPublication struct{ reader *repairReplayReader }

func (p *repairReplayPublication) Close() {}
func (p *repairReplayPublication) Publish(fn func()) error {
	if p.reader.publishError != nil {
		return p.reader.publishError
	}
	fn()
	return nil
}

func TestConsumerLogRepairTracksAllPartitionsAndLatestGeneration(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	defer release()
	first, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(first, nil)
	second, err := c.BeginConsumerLogRepair(1)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(second, nil)
	reader := &repairReplayReader{strictReplayReader: strictReader(c), verified: map[int]uint64{0: first}}
	_, err = c.PrepareFullConsumerMetadataReplay(reader)
	require.Error(t, err, "one verified partition cannot discharge both repairs")
	reader.verified[1] = second
	prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	require.Error(t, c.ApplyFullConsumerMetadataReplay(prepared), "split publication cannot complete pending repairs")
	third, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(third, nil)
	require.Equal(t, third, c.PendingConsumerLogRepair(0))
	require.Equal(t, second, c.PendingConsumerLogRepair(1))
	require.Error(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	_, err = c.PrepareFullConsumerMetadataReplay(reader)
	require.Error(t, err, "old verified generation is insufficient")
	reader.verified[0] = third
	prepared, err = c.PrepareFullConsumerMetadataReplay(reader)
	require.NoError(t, err)
	reader.publishError = fmt.Errorf("disk changed after replay")
	require.Error(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	require.Equal(t, third, c.PendingConsumerLogRepair(0))
	require.Error(t, c.CompleteConsumerRecovery(third))
	reader.publishError = nil
	require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	require.Zero(t, c.PendingConsumerLogRepair(0))
	require.Zero(t, c.PendingConsumerLogRepair(1))
	require.Error(t, c.ConsumerOperationsReadinessError(), "lease still owns admission until bookkeeping finishes")
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestConsumerLogRepairSurvivesRestoreAndOrdinaryImport(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	defer release()
	generation, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(generation, nil)
	state, envelope, err := c.ExportStateWithRecovery()
	require.NoError(t, err)
	restore := c.BeginConsumerRestore()
	require.NoError(t, c.ImportStateWithRecovery(state, envelope))
	c.FinishConsumerRestore(restore, nil)
	require.Equal(t, generation, c.PendingConsumerLogRepair(0))
	_, err = c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.Error(t, err)
}
