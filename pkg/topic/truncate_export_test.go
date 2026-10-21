package topic

// InstallTruncateCrashObserver is available only in this package's test binary.
func InstallTruncateCrashObserver(manager *TopicManager, observer func(string)) {
	manager.truncatePhase = observer
	if manager.metadataStore != nil {
		manager.metadataStore.phase = observer
	}
}
