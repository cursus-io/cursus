package metrics

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RecoveryStage and RecoveryUnit restrict labels to process-wide categories.
// Identities, paths, topics, partitions and error text are never metric labels.
type RecoveryStage uint8
type RecoveryUnit uint8

const (
	RecoveryFullExport RecoveryStage = iota
	RecoveryRetainedExport
	RecoveryRetainedPage
	RecoveryChecksum
	RecoveryChunkRead
	RecoveryChunkReceive
	RecoveryDetachedCopy
	RecoveryDetachedRead
	RecoveryMetadataRPC
	RecoveryReplicaRPC
	RecoveryRetainedScan
	RecoveryBootstrapFile
	RecoveryLineage
	RecoveryCatchupRPC
	RecoveryBootstrapInstall
	RecoveryRetentionTime
	RecoveryConsumerReplay
	RecoveryConsumerIndex
)

const (
	RecoveryReadBytes RecoveryUnit = iota
	RecoveryWriteBytes
	RecoveryHashBytes
	RecoveryTransferBytes
	RecoveryDecodedFrames
	RecoveryDecodedBytes
	RecoverySyncCalls
	RecoveryPages
	RecoveryCacheHits
	RecoveryCacheMisses
	RecoveryRetries
	RecoveryCanonicalHashBytes
)

var recoveryStages = [...]string{"full_export", "retained_export", "retained_page", "checksum", "chunk_read", "chunk_receive", "detached_copy", "detached_read", "metadata_rpc", "replica_rpc", "retained_scan", "bootstrap_file", "lineage", "catchup_rpc", "bootstrap_install", "retention_time", "consumer_replay", "consumer_index"}
var recoveryUnits = [...]string{"read_bytes", "write_bytes", "hash_bytes", "transfer_bytes", "decoded_frames", "decoded_bytes", "sync_calls", "pages", "cache_hits", "cache_misses", "retries", "canonical_hash_bytes"}

var recoveryWork = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursus_recovery_work_total",
	Help: "Logical recovery work, including failed and retried work; file bytes are not physical device I/O.",
}, []string{"stage", "unit"})

var recoveryAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursus_recovery_attempts_total",
	Help: "Completed recovery calls by bounded stage and outcome, including cancellation.",
}, []string{"stage", "outcome"})

func RecordRecoveryWork(stage RecoveryStage, unit RecoveryUnit, amount uint64) {
	if int(stage) < len(recoveryStages) && int(unit) < len(recoveryUnits) && amount > 0 {
		recoveryWork.WithLabelValues(recoveryStages[stage], recoveryUnits[unit]).Add(float64(amount))
	}
}

// RecordRecoveryBytes accepts actual I/O return counts, including partial errors.
func RecordRecoveryBytes(stage RecoveryStage, unit RecoveryUnit, amount int64) {
	if amount > 0 {
		RecordRecoveryWork(stage, unit, uint64(amount))
	}
}

func RecordRecoveryAttempt(stage RecoveryStage, err error) {
	if int(stage) >= len(recoveryStages) {
		return
	}
	outcome := "success"
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		outcome = "canceled"
	} else if err != nil {
		outcome = "failure"
	}
	recoveryAttempts.WithLabelValues(recoveryStages[stage], outcome).Inc()
}
