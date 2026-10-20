package controller

import (
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

// restageLocalEnds is called only inside the final FSM fence. Local views have
// not been read yet: move their replay bounds to this exact metadata cut, then
// require durable coverage when opening the views. Merely accepting a newer HWM
// while keeping an old replay bound would silently omit acknowledged records.
// Detached remote ranges and a replacement target cannot be extended here.
// They retain their original metadata and must pass the full vector check.
func (p *consumerRecoveryPlan) restageLocalEnds(metadata []fsm.PartitionMetadata) error {
	if len(p.metadata) != len(metadata) {
		return ErrConsumerRecoveryPending
	}
	for i, current := range metadata {
		before := p.metadata[i]
		if current.CommittedHWM < before.CommittedHWM {
			return fmt.Errorf("%w: partition %d committed end regressed", ErrConsumerRecoveryPending, i)
		}
		current.CommittedHWM = before.CommittedHWM
		if !reflect.DeepEqual(before, current) {
			return fmt.Errorf("%w: partition %d authority changed", ErrConsumerRecoveryPending, i)
		}
	}
	for i := range metadata {
		if p.local[i] != nil {
			p.metadata[i] = metadata[i]
			if !p.legacy {
				p.ends[i] = metadata[i].CommittedHWM
			}
		}
	}
	return nil
}
