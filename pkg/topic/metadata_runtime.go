package topic

import "errors"

type orphanedTopicMetadataError struct {
	count int
	err   error
}

func (e *orphanedTopicMetadataError) Error() string { return e.err.Error() }
func (e *orphanedTopicMetadataError) Unwrap() error { return e.err }

func confirmedOrphanCount(err error) int {
	var orphanErr *orphanedTopicMetadataError
	if errors.As(err, &orphanErr) {
		return orphanErr.count
	}
	return 0
}

func (tm *TopicManager) recordMetadataLoadResult(err error, orphanCount int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.metadataLoadFailure = ""
	if err != nil {
		tm.metadataLoadFailure = err.Error()
	}
	tm.metadataOrphanTopicCount = orphanCount
}

// recordMetadataDurabilityWarningLocked requires tm.mu to be held for writing.
func (tm *TopicManager) recordMetadataDurabilityWarningLocked(err error) {
	tm.metadataDurabilityWarning = err.Error()
	tm.metadataDurabilityWarningsTotal++
}
