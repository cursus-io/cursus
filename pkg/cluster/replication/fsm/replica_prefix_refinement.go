package fsm

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/disk"
)

// ValidateReplicaPrefixContinuationContext permits reuse of a read-only search
// only while its original metadata authority and receiver content still match.
// It does not authorize an apply; every retried response must be fenced again.
func (f *BrokerFSM) ValidateReplicaPrefixContinuationContext(ctx context.Context, request ReplicaCatchupRequest) error {
	if request.RecoveryProtocol != IndexedRecoveryProtocolVersion || request.localAuthority == nil || request.LocalWitness == nil {
		return fmt.Errorf("prefix continuation requires the original recovery witness")
	}
	return f.WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error {
		return request.localAuthority.partition.ValidateReplicaRequestWitness(ctx, request.LocalWitness, request.NextOffset)
	})
}

// ReplicaPrefixRefinement finds the next rejected candidate above the source's
// longest match. Call only after validating the response against this request.
// Adjacent boundaries prove that no additional common record will be removed.
func ReplicaPrefixRefinement(request ReplicaCatchupRequest, batch ReplicaCatchupBatch) (uint64, bool) {
	if batch.RecoveryProtocol != IndexedRecoveryProtocolVersion || batch.TruncateTo == nil || batch.MatchedPrefix == nil {
		return 0, false
	}
	lower, upper := batch.MatchedPrefix.EndOffset, request.NextOffset
	for _, candidate := range request.PrefixCandidates {
		if candidate.EndOffset > lower && candidate.EndOffset < upper {
			upper = candidate.EndOffset
		}
	}
	return upper, upper > lower && upper-lower > 1
}

// RefineReplicaPrefixContext preserves the original request witness and LEO.
// Only the read-only challenge changes; no speculative truncation is needed to
// discover an exact common prefix. The existing mode-1 source needs no upgrade.
func (f *BrokerFSM) RefineReplicaPrefixContext(ctx context.Context, request ReplicaCatchupRequest, batch ReplicaCatchupBatch) (ReplicaCatchupRequest, error) {
	upper, needed := ReplicaPrefixRefinement(request, batch)
	if !needed || request.localAuthority == nil || request.LocalWitness == nil {
		return ReplicaCatchupRequest{}, fmt.Errorf("prefix refinement requires the original recovery witness and interval")
	}
	p := request.localAuthority.partition
	check := func() error {
		return f.WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error {
			return p.ValidateReplicaRequestWitness(ctx, request.LocalWitness, request.NextOffset)
		})
	}
	if err := check(); err != nil {
		return ReplicaCatchupRequest{}, err
	}
	lower := batch.MatchedPrefix.EndOffset
	probe, err := p.ReplicaLineagePrefixContext(ctx, lower+(upper-lower)/2)
	if err != nil {
		return ReplicaCatchupRequest{}, err
	}
	if err := check(); err != nil {
		return ReplicaCatchupRequest{}, err
	}
	// Retain both the upper mismatch and exact tail. Forgetting the upper
	// endpoint would restart the search after a midpoint mismatch.
	proofs := []disk.LineagePrefix{probe, *batch.MatchedPrefix}
	for _, candidate := range request.PrefixCandidates {
		if candidate.EndOffset == request.NextOffset || candidate.EndOffset == upper {
			proofs = append(proofs, candidate)
		}
	}
	request.PrefixCandidates = proofs
	return request, nil
}
