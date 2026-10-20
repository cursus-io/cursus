package controller

import (
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
)

func (r *consumerRecoveryReader) ValidateConsumerLogRepair(partition int, repair uint64) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if partition < 0 || partition >= len(r.views) || r.views[partition] == nil || partition >= len(r.metadata) {
		return fmt.Errorf("pending consumer log repair lacks its original local read view")
	}
	metadata := r.metadata[partition]
	generation, err := r.views[partition].ReplicaRepairGeneration(metadata.LeaderEpoch, metadata.LifecycleEpoch, metadata.CommittedHWM, repair)
	if err != nil {
		return err
	}
	if previous, ok := r.repairGenerations[partition]; ok && previous != generation {
		return disk.ErrStaleContentGeneration
	}
	if r.repairGenerations == nil {
		r.repairGenerations = make(map[int]disk.ContentGeneration)
	}
	r.repairGenerations[partition] = generation
	return nil
}

type consumerRepairPublication struct {
	generations []disk.ContentGeneration
	releases    []func()
}

func (p *consumerRepairPublication) Publish(fn func()) error {
	return disk.TryPublishIfContentGenerations(p.generations, fn)
}
func (p *consumerRepairPublication) Close() {
	for i := len(p.releases) - 1; i >= 0; i-- {
		p.releases[i]()
	}
	p.releases = nil
}

func (r *consumerRecoveryReader) AcquireConsumerLogRepairPublication(repairs map[int]uint64) (coordinator.ConsumerLogRepairPublication, error) {
	ids := make([]int, 0, len(repairs))
	for partition := range repairs {
		ids = append(ids, partition)
	}
	sort.Ints(ids)
	publication := &consumerRepairPublication{}
	for _, partition := range ids {
		if err := r.ValidateConsumerLogRepair(partition, repairs[partition]); err != nil {
			publication.Close()
			return nil, err
		}
		metadata := r.metadata[partition]
		generation := r.repairGenerations[partition]
		release, err := r.views[partition].AcquireReplicaRepairPublication(metadata.LeaderEpoch, metadata.LifecycleEpoch, metadata.CommittedHWM, repairs[partition], generation)
		if err != nil {
			publication.Close()
			return nil, err
		}
		publication.generations = append(publication.generations, generation)
		publication.releases = append(publication.releases, release)
	}
	return publication, nil
}
