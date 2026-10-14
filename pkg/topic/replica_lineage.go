package topic

import "github.com/cursus-io/cursus/pkg/disk"

type replicaLineageStorage interface {
	LineageCandidates(uint64) ([]disk.LineagePrefix, error)
	MatchLineageCandidates([]disk.LineagePrefix, uint64) (*disk.LineagePrefix, error)
	VerifyLineagePrefix(disk.LineagePrefix, uint64) (bool, error)
}

// ReplicaLineageCandidates returns durable cumulative proofs of the retained log.
func (p *Partition) ReplicaLineageCandidates(tail uint64) ([]disk.LineagePrefix, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return nil, disk.ErrLineageUnsupported
	}
	return storage.LineageCandidates(tail)
}

func (p *Partition) MatchReplicaLineage(candidates []disk.LineagePrefix, hwm uint64) (*disk.LineagePrefix, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return nil, disk.ErrLineageUnsupported
	}
	return storage.MatchLineageCandidates(candidates, hwm)
}

func (p *Partition) VerifyReplicaLineage(prefix disk.LineagePrefix, hwm uint64) (bool, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return false, disk.ErrLineageUnsupported
	}
	return storage.VerifyLineagePrefix(prefix, hwm)
}
