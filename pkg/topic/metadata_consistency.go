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

func validateTopicStorageRoot(root string) error {
	info, err := os.Stat(root)
	switch {
	case err == nil && !info.IsDir():
		return fmt.Errorf("scan topic storage root: log path is not a directory")
	case err == nil:
		return nil
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("inspect topic storage root: %w", err)
	}
}

func (s *topicMetadataStore) orphanedTopicDirectories(manifestTopics map[string]struct{}) ([]string, error) {
	root := filepath.Dir(s.path)
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		info, statErr := os.Stat(root)
		switch {
		case statErr == nil && !info.IsDir():
			return nil, fmt.Errorf("scan topic storage root: log path is not a directory")
		case statErr == nil:
			return nil, nil
		case errors.Is(statErr, os.ErrNotExist):
			return nil, nil
		default:
			return nil, fmt.Errorf("inspect topic storage root: %w", statErr)
		}
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

func validateDeclaredTopicStorage(root string, definitions []Definition) error {
	for _, definition := range definitions {
		topicPath := filepath.Join(root, definition.Name)
		info, err := os.Lstat(topicPath)
		if err != nil {
			return fmt.Errorf("manifest topic %q has missing or inaccessible persisted storage: %w", definition.Name, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("manifest topic %q storage path is not a real directory", definition.Name)
		}
		entries, err := os.ReadDir(topicPath)
		if err != nil {
			return fmt.Errorf("read manifest topic %q storage: %w", definition.Name, err)
		}
		partitions := make(map[int]struct{}, definition.Partitions)
		for _, entry := range entries {
			name := entry.Name()
			matches := persistedSegmentName.FindStringSubmatch(name)
			if matches == nil {
				if strings.HasPrefix(name, "partition_") && strings.HasSuffix(name, ".log") {
					return fmt.Errorf("manifest topic %q has non-canonical persisted segment %q", definition.Name, name)
				}
				continue
			}
			if matches[3] != "" {
				continue
			}
			entryInfo, statErr := entry.Info()
			if statErr != nil {
				return fmt.Errorf("inspect manifest topic %q segment %q: %w", definition.Name, name, statErr)
			}
			if !entryInfo.Mode().IsRegular() || entryInfo.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("manifest topic %q segment %q is not a regular file", definition.Name, name)
			}
			partition, parseErr := strconv.Atoi(matches[1])
			if parseErr != nil || partition < 0 || partition >= definition.Partitions {
				return fmt.Errorf("manifest topic %q has persisted partition outside [0,%d): %q", definition.Name, definition.Partitions, name)
			}
			partitions[partition] = struct{}{}
		}
		for partition := 0; partition < definition.Partitions; partition++ {
			if _, exists := partitions[partition]; !exists {
				return fmt.Errorf("manifest topic %q partition %d has no active persisted log", definition.Name, partition)
			}
		}
	}
	return nil
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
