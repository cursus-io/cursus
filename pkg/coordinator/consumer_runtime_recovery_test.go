package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestStartupRequirementSurvivesOrdinaryReloadAndLeaseRelease(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	require.ErrorIs(t, c.ReloadDistributedConsumerMetadata(), ErrConsumerMetadataRecovering)
	c.markRecoveryComplete(ConsumerMetadataRecoveryStatus{})
	release := acquireReplay(t, c)
	release()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	require.Error(t, c.RecoveryReadinessError())
}

func TestRestoreGenerationInvalidatesPreparationBeforeImport(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	p, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	generation := c.BeginConsumerRestore()
	require.True(t, c.ConsumerRecoveryRequirement().Installing)
	require.Error(t, c.ApplyFullConsumerMetadataReplay(p))
	c.FinishConsumerRestore(generation, nil)
	require.Error(t, c.CompleteConsumerRecovery(generation))
	p, err = c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	next := c.BeginConsumerRestore()
	c.FinishConsumerRestore(next, errors.New("import failed"))
	require.Error(t, c.CompleteConsumerRecovery(generation))
	release()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	require.ErrorContains(t, c.ConsumerRecoveryRequirement().Failure, "import failed")
}

func TestStrictCompletionOpensOnlyMatchingGenerationAfterRelease(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	release := acquireReplay(t, c)
	p, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	require.NoError(t, c.CompleteConsumerRecovery(generation))
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestLegacyOriginalStartupCompletesWithoutGainingBootstrapAuthority(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportStateWithRecovery(nil, nil))
	release := acquireReplay(t, c)
	p, err := c.PrepareStartupConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	require.NoError(t, c.CompleteConsumerRecovery(c.ConsumerRecoveryRequirement().RequiredGeneration))
	release()
	require.NoError(t, c.RecoveryReadinessError())
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
	require.NoError(t, c.ValidateConsumerRestoreOrigin(nil))
	release = acquireReplay(t, c)
	_, err = c.PrepareLegacyConsumerMetadataReplay(strictReader(c))
	require.ErrorIs(t, err, ErrConsumerMetadataBootstrapUnknown, "ordinary reload cannot select compatibility recovery")
	release()
	for range 2 {
		generation := c.BeginConsumerRestore()
		_, err = c.PrepareLegacyConsumerMetadataReplay(strictReader(c))
		require.Error(t, err, "restore must finish and acquire an exclusive lease")
		require.NoError(t, c.ImportStateWithRecovery(nil, nil))
		c.FinishConsumerRestore(generation, nil)
		release = acquireReplay(t, c)
		p, err = c.PrepareLegacyConsumerMetadataReplay(strictReader(c))
		require.NoError(t, err)
		require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
		require.NoError(t, c.CompleteConsumerRecovery(generation))
		release()
		require.NoError(t, c.RecoveryReadinessError())
		require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), ErrConsumerMetadataBootstrapUnknown)
	}
}

func TestCompletionCannotReuseInstallationFromReleasedLease(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	release := acquireReplay(t, c)
	p, err := c.PrepareFullConsumerMetadataReplay(strictReader(c))
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	release()
	acquireReplay(t, c)
	require.Error(t, c.CompleteConsumerRecovery(generation))
}

func TestKnownStartupCannotSelectLegacyOverlayReplay(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	acquireReplay(t, c)
	_, err = c.PrepareStartupConsumerMetadataReplay(strictReader(c))
	require.Error(t, err)
}

func TestLegacyRestoreReplayRejectsChangedGenerationOrAuthority(t *testing.T) {
	for _, mutation := range []string{"restore", "authority", "failure"} {
		t.Run(mutation, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.ImportStateWithRecovery(nil, nil))
			generation := c.BeginConsumerRestore()
			c.FinishConsumerRestore(generation, nil)
			acquireReplay(t, c)
			prepared, err := c.PrepareLegacyConsumerMetadataReplay(strictReader(c))
			require.NoError(t, err)
			switch mutation {
			case "restore":
				next := c.BeginConsumerRestore()
				c.FinishConsumerRestore(next, nil)
			case "authority":
				known := newAuthorityTestCoordinator(t)
				groups, proof, err := known.ExportStateWithRecovery()
				require.NoError(t, err)
				require.NoError(t, c.ImportStateWithRecovery(groups, proof))
				require.ErrorIs(t, c.ValidateConsumerRestoreOrigin(nil), ErrConsumerMetadataBootstrapUnknown)
				_, err = c.PrepareLegacyConsumerMetadataReplay(strictReader(c))
				require.Error(t, err, "a known envelope cannot use compatibility replay")
			case "failure":
				c.FinishConsumerRestore(generation, errors.New("restore failed"))
			}
			before := c.ExportState()
			require.Error(t, c.ApplyFullConsumerMetadataReplay(prepared))
			require.Equal(t, before, c.ExportState())
			require.Error(t, c.CompleteConsumerRecovery(generation))
		})
	}
}
