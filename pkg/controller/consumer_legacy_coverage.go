package controller

import "fmt"

// Unknown source provenance requires ALL physical partitions. An existing
// snapshot may seed compatibility replay, but cannot certify a discarded prefix
// or hole. Until such inputs have independently verifiable coverage, reject
// them without publishing an overlay or changing the unknown authority flags.
func (r *consumerRecoveryReader) ValidateFullCommittedCoverage() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if len(r.views) == 0 || len(r.views) != len(r.metadata) || len(r.ends) != len(r.views) || r.staged != nil {
		return fmt.Errorf("%w: legacy full source coverage unavailable", ErrConsumerRecoveryPending)
	}
	for p, view := range r.views {
		if view == nil || view.FirstOffset() != 0 || r.ends[p] != r.metadata[p].CommittedHWM || view.CommittedEnd() < r.ends[p] {
			return fmt.Errorf("%w: legacy partition %d committed coverage unavailable", ErrConsumerRecoveryPending, p)
		}
		if _, err := view.OffsetRange(); err != nil {
			return fmt.Errorf("%w: legacy partition %d read view expired", ErrConsumerRecoveryPending, p)
		}
	}
	if r.legacyCoverageValidated {
		// This certificate exists only within these held read views/FSM fence.
		return nil
	}
	for p, view := range r.views {
		for next := uint64(0); next < r.ends[p]; {
			if err := r.ctx.Err(); err != nil {
				return err
			}
			records, _, err := view.ReadMessagesBounded(next, 1024, 1<<20, true)
			r.observeReplayRead(records)
			if err != nil {
				return fmt.Errorf("%w: legacy partition %d physical read failed", ErrConsumerRecoveryPending, p)
			}
			if len(records) == 0 {
				return fmt.Errorf("%w: legacy partition %d committed tail missing at %d", ErrConsumerRecoveryPending, p, next)
			}
			for _, record := range records {
				if next == r.ends[p] {
					break
				}
				if record.Offset != next {
					return fmt.Errorf("%w: legacy partition %d committed gap at %d", ErrConsumerRecoveryPending, p, next)
				}
				next++
			}
		}
	}
	r.legacyCoverageValidated = true
	return nil
}
