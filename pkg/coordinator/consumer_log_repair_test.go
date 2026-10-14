package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConsumerLogRepairFinishCannotClearOtherGenerationOrFailure(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	generation, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(generation+1, nil)
	require.True(t, c.ConsumerRecoveryRequirement().Installing)
	failure := errors.New("uncertain disk mutation")
	c.FinishConsumerLogRepair(generation, failure)
	c.FinishConsumerLogRepair(generation, nil)
	require.ErrorIs(t, c.ConsumerRecoveryRequirement().Failure, failure)
	release()
	release = acquireReplay(t, c)
	_, err = c.BeginConsumerLogRepair(1)
	require.Error(t, err)
	c.FinishConsumerLogRepair(generation, nil)
	release()
	require.ErrorIs(t, c.ConsumerRecoveryRequirement().Failure, failure)
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
}

func TestConsumerLogRepairCleanRejectionRequiresStrictReplay(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	generation, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(generation, nil)
	release()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	release = acquireReplay(t, c)
	require.Error(t, c.CompleteConsumerRecovery(generation))
	prepared, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(prepared))
	require.NoError(t, c.CompleteConsumerRecovery(generation))
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestConsumerLogRepairRejectsUnknownOriginalStartupAuthority(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportStateWithRecovery(nil, nil))
	acquireReplay(t, c)
	require.True(t, c.ConsumerRecoveryRequirement().StartupPending)
	before := c.ConsumerRecoveryRequirement().RequiredGeneration
	_, err = c.BeginConsumerLogRepair(0)
	require.ErrorIs(t, err, ErrConsumerMetadataBootstrapUnknown)
	require.Equal(t, before, c.ConsumerRecoveryRequirement().RequiredGeneration)
}
