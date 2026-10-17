package controller

import (
	"context"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type bindingSource struct {
	entered chan struct{}
	resume  chan struct{}
}

func (s *bindingSource) BeginBootstrapTransfer(context.Context, fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	close(s.entered)
	<-s.resume
	return fsm.BootstrapBeginResponse{}, fsm.ErrBootstrapTransferStaleFence
}
func (s *bindingSource) ReadBootstrapTransferChunk(context.Context, fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
	return fsm.BootstrapChunkResponse{}, nil
}
func (s *bindingSource) ReleaseBootstrapTransfer(context.Context, fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	return fsm.BootstrapReleaseResponse{}, nil
}

func TestBootstrapBindingOnceWithoutHoldingLockDuringIO(t *testing.T) {
	sd := NewServiceDiscoveryImpl(nil, "source", "", "")
	_, err := sd.BeginBootstrapTransfer(context.Background(), fsm.BootstrapBeginRequest{})
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
	require.Error(t, sd.BindBootstrapTransferSource(nil))
	var absent *bindingSource
	require.Error(t, sd.BindBootstrapTransferSource(absent))
	source := &bindingSource{make(chan struct{}), make(chan struct{})}
	require.NoError(t, sd.BindBootstrapTransferSource(source))
	done := make(chan error, 1)
	go func() {
		_, err := sd.BeginBootstrapTransfer(context.Background(), fsm.BootstrapBeginRequest{})
		done <- err
	}()
	<-source.entered
	rebound := make(chan error, 1)
	go func() { rebound <- sd.BindBootstrapTransferSource(source) }()
	select {
	case err := <-rebound:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("binding lock held during capture")
	}
	close(source.resume)
	require.ErrorIs(t, <-done, fsm.ErrBootstrapTransferStaleFence)
}
