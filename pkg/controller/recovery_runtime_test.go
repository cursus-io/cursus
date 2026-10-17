package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryRuntimeRequiresCompleteCurrentProcessInstallation(t *testing.T) {
	receiver, _, client, _, _ := replicaReceiverFixture(t)
	require.Error(t, receiver.RecoveryRuntimeReady("requester-process"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, receiver.ConfigureBootstrapTransferSource(ctx, "requester-process"))
	require.Error(t, receiver.RecoveryRuntimeReady("requester-process"), "metadata recovery provider is still absent")
	require.NoError(t, receiver.ConfigureConsumerMetadataRecovery(ctx, "requester-process", client))
	require.NoError(t, receiver.RecoveryRuntimeReady("requester-process"))
	require.Error(t, receiver.RecoveryRuntimeReady("old-process"))
	cancel()
	require.Error(t, receiver.RecoveryRuntimeReady("requester-process"))
	require.NoError(t, receiver.Close())
	require.Error(t, receiver.RecoveryRuntimeReady("requester-process"))
}
