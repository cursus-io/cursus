package coordinator

import (
	"fmt"
	"maps"
	"reflect"
)

// A mixed legacy snapshot is a valid seed for Raft-only state, but a modern
// acknowledged revision cannot be silently discarded or carried beyond the
// certified committed log cut. Later lifecycle epochs/deletions supersede it.
func (c *Coordinator) validateLegacyCommittedProjection(groups map[string]*GroupMetadata, epochs map[string]uint64) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for name, previous := range c.groups {
		if previous == nil {
			continue
		}
		previous.mu.RLock()
		next := groups[name]
		epoch, present := epochs[name]
		superseded := present && (epoch > previous.RegistrationEpoch || next == nil && epoch >= previous.RegistrationEpoch)
		if !superseded {
			for topic, revision := range previous.OffsetRevisions {
				if revision == 0 {
					continue
				}
				if next == nil || next.RegistrationEpoch != previous.RegistrationEpoch || next.OffsetRevisions[topic] < revision ||
					(next.OffsetRevisions[topic] == revision && !maps.Equal(next.Offsets[topic], previous.Offsets[topic])) {
					previous.mu.RUnlock()
					return fmt.Errorf("legacy consumer snapshot offset revision lacks matching committed evidence")
				}
				for partition, offset := range previous.Offsets[topic] {
					if recovered, ok := next.Offsets[topic][partition]; !ok || recovered < offset {
						previous.mu.RUnlock()
						return fmt.Errorf("legacy consumer snapshot offset would regress within its registration epoch")
					}
				}
			}
			if previous.ReservationRevision > 0 && (next == nil || next.RegistrationEpoch != previous.RegistrationEpoch || next.ReservationRevision < previous.ReservationRevision) {
				previous.mu.RUnlock()
				return fmt.Errorf("legacy consumer snapshot reservation revision lacks committed evidence")
			}
			if previous.ReservationRevision > 0 && next.ReservationRevision == previous.ReservationRevision &&
				(!reflect.DeepEqual(cloneOffsetReservations(next.OffsetReservations), cloneOffsetReservations(previous.OffsetReservations)) ||
					!maps.Equal(next.ReservationDecisions, previous.ReservationDecisions)) {
				previous.mu.RUnlock()
				return fmt.Errorf("legacy consumer snapshot reservation revision conflicts with committed evidence")
			}
		}
		previous.mu.RUnlock()
	}
	return nil
}

// ConsumerMetadataFullCoverageReader certifies every physical source partition
// through an authoritative committed cut, including transaction-hidden records.
// The caller must hold the FSM transition fence and immutable local views for
// the complete prepare/publish operation. Certification must reject gaps and
// unavailable history; visible records alone cannot establish coverage.
type ConsumerMetadataFullCoverageReader interface {
	ConsumerMetadataCommittedReader
	ValidateFullCommittedCoverage() error
}

// PrepareFencedLegacyConsumerMetadataReplay recovers the existing snapshot
// overlay without claiming a historical Raft baseline or source provenance.
// Unlike the startup-only API, it supports ownership changes after activation,
// but only with complete physical coverage under the caller's FSM fence.
// Destructive/scoped bootstrap continues to require known authority.
func (c *Coordinator) PrepareFencedLegacyConsumerMetadataReplay(reader ConsumerMetadataFullCoverageReader) (*PreparedConsumerMetadataReplay, error) {
	stamp, err := c.captureConsumerMetadataReplayStampForRecovery(true, true)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, ErrConsumerMetadataBootstrapUnknown
	}
	if err := reader.ValidateFullCommittedCoverage(); err != nil {
		return nil, err
	}
	repairs := c.pendingConsumerLogRepairs()
	validator, _ := reader.(ConsumerLogRepairValidator)
	if err := validateConsumerLogRepairs(validator, repairs); err != nil {
		return nil, err
	}
	prepared, err := c.prepareConsumerMetadataReplay(reader, nil, stamp)
	if err != nil {
		return nil, err
	}
	if err := reader.ValidateFullCommittedCoverage(); err != nil {
		return nil, err
	}
	if err := validateConsumerLogRepairs(validator, repairs); err != nil {
		return nil, err
	}
	prepared.repairs, prepared.repairValidator = repairs, validator
	prepared.legacyCoverage = reader
	return prepared, nil
}
