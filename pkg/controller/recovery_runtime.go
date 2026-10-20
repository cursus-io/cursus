package controller

import "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"

// RecoveryRuntimeReady verifies installation before process capability is
// published. Registration is intentionally not required at this boundary.
func (ch *CommandHandler) RecoveryRuntimeReady(incarnation string) error {
	if ch == nil || incarnation == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	ch.consumerRecoveryInitMu.Lock()
	defer ch.consumerRecoveryInitMu.Unlock()
	ch.bootstrapMu.Lock()
	defer ch.bootstrapMu.Unlock()
	source, cache, receiver := ch.bootstrapSource, ch.consumerRecoveryCache, ch.replicaBootstrap
	if ch.closed || source == nil || cache == nil || receiver == nil || source.replicaCapture == nil ||
		source.incarnation != incarnation || cache.incarnation != incarnation || receiver.incarnation != incarnation ||
		source.ctx.Err() != nil || cache.ctx.Err() != nil || receiver.ctx.Err() != nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	return nil
}
