package controller

import (
	"context"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/recovery"
)

// SetRecoveryBudget must run before configuring bootstrap or serving requests.
func (ch *CommandHandler) SetRecoveryBudget(b *recovery.Budget) { ch.recoveryBudget = b }

// Admission is nonblocking and precedes the snapshot catalog/storage gates.
// Legacy callers share one source bucket instead of creating unbounded labels.
func (ch *CommandHandler) handleBudgetedSnapshotRead(ctx context.Context, cmd string, read func(string) string) string {
	if ch.recoveryBudget == nil {
		return read(cmd)
	}
	_, arguments, _ := strings.Cut(cmd, " ")
	source := parseKeyValueArgs(arguments)["recovery_requester"]
	if source == "" {
		source = "legacy-snapshot"
	}
	lease, _, err := ch.recoveryBudget.TryAcquireEstimated(ctx, source, ch.Config.RecoverySettings().QuantumBytes)
	if err != nil {
		return "ERROR: recovery_busy"
	}
	var size int64
	defer func() { recovery.SettleAfterResponse(ctx, func() { _ = lease.Finish(size) }) }()
	response := read(cmd)
	size = int64(len(response))
	return response
}

func (r *replicaBootstrapRecovery) readBudgetedChunk(ctx context.Context, address string, request fsm.BootstrapChunkRequest, fileSize int64, source fsm.BootstrapProcessIdentity) (fsm.BootstrapChunkResponse, error) {
	return r.ch.readBudgetedBootstrapChunk(ctx, r.client, address, r.port, request, fileSize, source)
}

func (ch *CommandHandler) readBudgetedBootstrapChunk(ctx context.Context, client ConsumerMetadataRecoveryClient, address string, port int, request fsm.BootstrapChunkRequest, fileSize int64, source fsm.BootstrapProcessIdentity) (fsm.BootstrapChunkResponse, error) {
	if ch.recoveryBudget == nil {
		return client.ReadBootstrapTransferChunk(ctx, address, port, request, fileSize)
	}
	key := source.BrokerID
	// JSON base64 data plus bounded manifest identities fit this reservation.
	lease, err := ch.recoveryBudget.Acquire(ctx, key, int64(request.Size)*2+4096)
	if err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	ctx, meter := recovery.Measure(ctx)
	defer func() { _ = lease.Finish(meter.Bytes()) }()
	response, err := client.ReadBootstrapTransferChunk(ctx, address, port, request, fileSize)
	if meter.Bytes() == 0 {
		recovery.RecordTransfer(ctx, len(response.Data)*4/3+4096)
	}
	return response, err
}
