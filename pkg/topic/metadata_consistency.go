package topic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type topicMetadataSaveError struct {
	committed bool
	err       error
}

func (e *topicMetadataSaveError) Error() string {
	return e.err.Error()
}

func (e *topicMetadataSaveError) Unwrap() error {
	return e.err
}

func topicMetadataWriteCommitted(err error) bool {
	var saveErr *topicMetadataSaveError
	return errors.As(err, &saveErr) && saveErr.committed
}

func (s *topicMetadataStore) orphanedTopicDirectories(manifestTopics map[string]struct{}) ([]string, error) {
	root := filepath.Dir(s.path)
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan topic storage root: %w", err)
	}

	orphaned := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if _, exists := manifestTopics[name]; exists {
			continue
		}
		if ValidateName(name) != nil {
			continue
		}
		persisted, inspectErr := hasPersistedPartitionLog(filepath.Join(root, name))
		if inspectErr != nil {
			return nil, fmt.Errorf("inspect topic storage %q: %w", name, inspectErr)
		}
		if persisted {
			orphaned = append(orphaned, name)
		}
	}
	sort.Strings(orphaned)
	return orphaned, nil
}

// PersistedTopicStorageNames returns topic directories that contain partition
// logs without opening handlers. Distributed snapshot restore uses this to
// resume cleanup work even after an in-memory delete issue was lost on restart.
func (tm *TopicManager) PersistedTopicStorageNames() ([]string, error) {
	if tm == nil || tm.cfg == nil || strings.TrimSpace(tm.cfg.LogDir) == "" {
		return nil, nil
	}
	store := &topicMetadataStore{path: filepath.Join(tm.cfg.LogDir, TopicMetadataFileName)}
	return store.orphanedTopicDirectories(nil)
}
func hasPersistedPartitionLog(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "partition_") || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		remainder := strings.TrimPrefix(entry.Name(), "partition_")
		separator := strings.Index(remainder, "_segment_")
		if separator <= 0 {
			continue
		}
		partition, parseErr := strconv.Atoi(remainder[:separator])
		if parseErr == nil && partition >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (tm *TopicManager) rejectOrphanedStorageLocked(name string) error {
	if tm.metadataStore == nil {
		return nil
	}
	provider, ok := tm.hp.(existingPartitionProvider)
	if !ok {
		return nil
	}
	partitions, err := provider.ExistingPartitionCount(name)
	if err != nil {
		return fmt.Errorf("inspect persisted topic storage %q: %w", name, err)
	}
	if partitions > 0 {
		return fmt.Errorf("topic %q has persisted storage without an active durable definition", name)
	}
	return nil
}
