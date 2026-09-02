package replication

import "sync/atomic"

const (
	ISRProofOutcomeAccepted = "accepted"
	ISRProofOutcomeRejected = "rejected"

	ISRProofReasonApplied          = "applied"
	ISRProofReasonAlreadyInISR     = "already_in_isr"
	ISRProofReasonIdentityMismatch = "identity_mismatch"
	ISRProofReasonFSMUnavailable   = "fsm_unavailable"
	ISRProofReasonValidation       = "validation"
	ISRProofReasonEncoding         = "encoding"
	ISRProofReasonApply            = "apply"
)

type isrProofMetricDefinition struct {
	outcome string
	reason  string
}

var isrProofMetricDefinitions = [...]isrProofMetricDefinition{
	{outcome: ISRProofOutcomeAccepted, reason: ISRProofReasonApplied},
	{outcome: ISRProofOutcomeAccepted, reason: ISRProofReasonAlreadyInISR},
	{outcome: ISRProofOutcomeRejected, reason: ISRProofReasonIdentityMismatch},
	{outcome: ISRProofOutcomeRejected, reason: ISRProofReasonFSMUnavailable},
	{outcome: ISRProofOutcomeRejected, reason: ISRProofReasonValidation},
	{outcome: ISRProofOutcomeRejected, reason: ISRProofReasonEncoding},
	{outcome: ISRProofOutcomeRejected, reason: ISRProofReasonApply},
}

var isrProofMetricCounters [len(isrProofMetricDefinitions)]atomic.Uint64

// ISRProofMetric is one bounded process-lifetime ISR proof result counter.
type ISRProofMetric struct {
	Outcome string
	Reason  string
	Count   uint64
}

// ISRProofMetrics returns every supported result label, including zero values.
func ISRProofMetrics() []ISRProofMetric {
	result := make([]ISRProofMetric, 0, len(isrProofMetricDefinitions))
	for index, definition := range isrProofMetricDefinitions {
		result = append(result, ISRProofMetric{
			Outcome: definition.outcome,
			Reason:  definition.reason,
			Count:   isrProofMetricCounters[index].Load(),
		})
	}
	return result
}

func recordISRProof(outcome, reason string) {
	for index, definition := range isrProofMetricDefinitions {
		if definition.outcome == outcome && definition.reason == reason {
			isrProofMetricCounters[index].Add(1)
			return
		}
	}
}
