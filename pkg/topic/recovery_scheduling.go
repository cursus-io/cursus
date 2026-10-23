package topic

// RecoverySchedulingPosition is an advisory, nonblocking observation. Discovery
// must not wait behind a partition's fsync/apply while unrelated work is ready.
// Unknown state is scheduled for checked maintenance/recovery, never treated as
// authority to mutate data or admit an ISR member.
func (p *Partition) RecoverySchedulingPosition() (leo, hwm uint64, known bool) {
	leo = p.LEO.Load()
	if !p.mu.TryRLock() {
		return leo, 0, false
	}
	defer p.mu.RUnlock()
	return leo, p.HWM, true
}
