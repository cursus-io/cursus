package stream

// ActiveCount returns the number of currently registered streaming consumers.
func (sm *StreamManager) ActiveCount() int {
	if sm == nil {
		return 0
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.streams)
}
