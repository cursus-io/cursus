package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DeleteTopic closes every handler owned by topic and removes its persisted log.
// The manager lock prevents a concurrent GetHandler from recreating a handler
// while deletion is in progress.
func (dm *DiskManager) DeleteTopic(topic string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var errs []error
	for key, dh := range dm.handlers {
		if !diskHandlerKeyMatchesTopic(key, topic) {
			continue
		}
		if err := dh.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close handler %s: %w", key, err))
		}
		delete(dm.handlers, key)
	}

	topicDir, err := dm.topicLogDir(topic)
	if err != nil {
		errs = append(errs, err)
	} else if err := os.RemoveAll(topicDir); err != nil {
		errs = append(errs, fmt.Errorf("remove topic log directory %s: %w", topicDir, err))
	}
	return errors.Join(errs...)
}

func (dm *DiskManager) topicLogDir(topic string) (string, error) {
	root, err := filepath.Abs(dm.cfg.LogDir)
	if err != nil {
		return "", fmt.Errorf("resolve log root: %w", err)
	}
	target, err := filepath.Abs(filepath.Join(root, topic))
	if err != nil {
		return "", fmt.Errorf("resolve topic log directory: %w", err)
	}
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", fmt.Errorf("compare topic log directory: %w", err)
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("refusing to delete topic path outside log root: %s", target)
	}
	return target, nil
}

func diskHandlerKeyMatchesTopic(key, topic string) bool {
	if !strings.HasPrefix(key, topic+"_") {
		return false
	}
	partition := strings.TrimPrefix(key, topic+"_")
	if partition == "" {
		return false
	}
	_, err := strconv.Atoi(partition)
	return err == nil
}
