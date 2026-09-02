package wire

import (
	"errors"
	"sync/atomic"
)

const (
	ProtocolFailureInvalidFrame        = "invalid_frame"
	ProtocolFailureFrameTooLarge       = "frame_too_large"
	ProtocolFailureChecksumMismatch    = "checksum_mismatch"
	ProtocolFailureCompressionMismatch = "compression_mismatch"
	ProtocolFailureDecompression       = "decompression_rejected"
	ProtocolFailureUnknown             = "unknown"

	DecompressionRejectionInvalidPayload = "invalid_payload"
)

var protocolFailureReasons = [...]string{
	ProtocolFailureInvalidFrame,
	ProtocolFailureFrameTooLarge,
	ProtocolFailureChecksumMismatch,
	ProtocolFailureCompressionMismatch,
	ProtocolFailureDecompression,
	ProtocolFailureUnknown,
}

var decompressionRejectionReasons = [...]string{
	DecompressionRejectionInvalidPayload,
}

var (
	protocolFailureCounters        [len(protocolFailureReasons)]atomic.Uint64
	decompressionRejectionCounters [len(decompressionRejectionReasons)]atomic.Uint64
)

// RuntimeMetricsSnapshot is the process-lifetime Wire v2 failure accounting.
// Every supported reason is present, including reasons whose count is zero.
type RuntimeMetricsSnapshot struct {
	ProtocolFailures        map[string]uint64
	DecompressionRejections map[string]uint64
}

// RuntimeMetrics returns a point-in-time snapshot of bounded Wire v2 counters.
func RuntimeMetrics() RuntimeMetricsSnapshot {
	snapshot := RuntimeMetricsSnapshot{
		ProtocolFailures:        make(map[string]uint64, len(protocolFailureReasons)),
		DecompressionRejections: make(map[string]uint64, len(decompressionRejectionReasons)),
	}
	for index, reason := range protocolFailureReasons {
		snapshot.ProtocolFailures[reason] = protocolFailureCounters[index].Load()
	}
	for index, reason := range decompressionRejectionReasons {
		snapshot.DecompressionRejections[reason] = decompressionRejectionCounters[index].Load()
	}
	return snapshot
}

func recordProtocolFailure(err error) {
	reason := ProtocolFailureUnknown
	switch {
	case errors.Is(err, ErrDecompressionRejected):
		reason = ProtocolFailureDecompression
	case errors.Is(err, ErrCompressionMismatch):
		reason = ProtocolFailureCompressionMismatch
	case errors.Is(err, ErrChecksumMismatch):
		reason = ProtocolFailureChecksumMismatch
	case errors.Is(err, ErrFrameTooLarge):
		reason = ProtocolFailureFrameTooLarge
	case errors.Is(err, ErrInvalidFrame):
		reason = ProtocolFailureInvalidFrame
	}
	incrementReason(protocolFailureReasons[:], protocolFailureCounters[:], reason)
}

func recordDecompressionRejection() {
	incrementReason(
		decompressionRejectionReasons[:],
		decompressionRejectionCounters[:],
		DecompressionRejectionInvalidPayload,
	)
}

func incrementReason(reasons []string, counters []atomic.Uint64, wanted string) {
	for index, reason := range reasons {
		if reason == wanted {
			counters[index].Add(1)
			return
		}
	}
}
