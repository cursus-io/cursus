package controller

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

// BootstrapTransferSource is optional so existing discovery implementations do
// not need to provide metadata exports.
type BootstrapTransferSource interface {
	BeginBootstrapTransfer(context.Context, fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error)
	ReadBootstrapTransferChunk(context.Context, fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error)
	ReleaseBootstrapTransfer(context.Context, fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error)
}

type BootstrapTransferSourceBinder interface {
	BindBootstrapTransferSource(BootstrapTransferSource) error
}

// ActivateRecoveryRuntime publishes one immutable process capability only after
// source, receiver and metadata recovery owners report a complete installation.
// The server calls this after installing the internal listener and scheduler.
func (sd *serviceDiscovery) ActivateRecoveryRuntime() error {
	sd.bootstrapMu.Lock()
	defer sd.bootstrapMu.Unlock()
	runtime, ok := sd.bootstrapSource.(interface{ RecoveryRuntimeReady(string) error })
	if !ok {
		return fsm.ErrBootstrapTransferUnavailable
	}
	if err := runtime.RecoveryRuntimeReady(sd.incarnationID); err != nil {
		return err
	}
	sd.recoveryProtocol = fsm.BootstrapRecoveryProtocolVersion
	return nil
}

func (sd *serviceDiscovery) BindBootstrapTransferSource(source BootstrapTransferSource) error {
	if source == nil {
		return fmt.Errorf("bootstrap source must not be nil")
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return fmt.Errorf("bootstrap source must not be nil")
		}
	}
	sd.bootstrapMu.Lock()
	defer sd.bootstrapMu.Unlock()
	if sd.bootstrapSource != nil {
		return fmt.Errorf("bootstrap source already bound")
	}
	sd.bootstrapSource = source
	return nil
}

func (sd *serviceDiscovery) bootstrapTransferSource() (BootstrapTransferSource, error) {
	sd.bootstrapMu.RLock()
	source := sd.bootstrapSource
	sd.bootstrapMu.RUnlock()
	if source == nil {
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	return source, nil
}
func (sd *serviceDiscovery) BeginBootstrapTransfer(ctx context.Context, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	source, err := sd.bootstrapTransferSource()
	if err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	return source.BeginBootstrapTransfer(ctx, r)
}
func (sd *serviceDiscovery) ReadBootstrapTransferChunk(ctx context.Context, r fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
	source, err := sd.bootstrapTransferSource()
	if err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	return source.ReadBootstrapTransferChunk(ctx, r)
}
func (sd *serviceDiscovery) ReleaseBootstrapTransfer(ctx context.Context, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	source, err := sd.bootstrapTransferSource()
	if err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	return source.ReleaseBootstrapTransfer(ctx, r)
}
