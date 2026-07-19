//go:build e2e_faults

package e2efaults

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

const (
	RuntimeGateEnvironment = "CURSUS_E2E_FAULTS"
	SentinelRoot           = "/run/cursus-e2e-faults"
	HandlerOpenOperation   = "handler-open"
	CleanupOperation       = "cleanup"
)

// StorageProvider injects node-local storage faults only in e2e_faults builds.
// Embedding preserves DiskManager's optional provider interfaces.
type StorageProvider struct {
	*disk.DiskManager
	logRoot      string
	sentinelRoot string
}

var (
	_ topic.HandlerProvider = (*StorageProvider)(nil)
	_ interface {
		GetHandlerWithPolicy(string, int, string, int, int64) (types.StorageHandler, error)
	} = (*StorageProvider)(nil)
	_ interface{ ExistingPartitionCount(string) (int, error) } = (*StorageProvider)(nil)
	_ interface{ TopicMetadataPath() string }                  = (*StorageProvider)(nil)
	_ interface{ CloseTopicHandlers(string) }                  = (*StorageProvider)(nil)
	_ interface{ ClosePartitionHandler(string, int) }          = (*StorageProvider)(nil)
	_ interface{ RemoveTopicStorage(string) error }            = (*StorageProvider)(nil)
)

func NewStorageProvider(dm *disk.DiskManager, logDir string) (*StorageProvider, error) {
	if os.Getenv(RuntimeGateEnvironment) != "1" {
		return nil, fmt.Errorf("e2e fault build requires %s=1", RuntimeGateEnvironment)
	}
	return newStorageProvider(dm, logDir, SentinelRoot)
}

func newStorageProvider(dm *disk.DiskManager, logDir, sentinelRoot string) (*StorageProvider, error) {
	if dm == nil {
		return nil, errors.New("disk manager is required")
	}
	logRoot, err := filepath.Abs(logDir)
	if err != nil {
		return nil, fmt.Errorf("resolve log root: %w", err)
	}
	if strings.TrimSpace(logDir) == "" || strings.TrimSpace(sentinelRoot) == "" {
		return nil, errors.New("log and sentinel roots are required")
	}
	return &StorageProvider{
		DiskManager:  dm,
		logRoot:      filepath.Clean(logRoot),
		sentinelRoot: filepath.Clean(sentinelRoot),
	}, nil
}

func (provider *StorageProvider) GetHandler(topicName string, partitionID int) (types.StorageHandler, error) {
	if err := provider.injectedFault(HandlerOpenOperation, topicName); err != nil {
		return nil, err
	}
	return provider.DiskManager.GetHandler(topicName, partitionID)
}

func (provider *StorageProvider) GetHandlerWithPolicy(topicName string, partitionID int, cleanupPolicy string, retentionHours int, retentionBytes int64) (types.StorageHandler, error) {
	if err := provider.injectedFault(HandlerOpenOperation, topicName); err != nil {
		return nil, err
	}
	return provider.DiskManager.GetHandlerWithPolicy(topicName, partitionID, cleanupPolicy, retentionHours, retentionBytes)
}

func (provider *StorageProvider) RemoveTopicStorage(path string) error {
	target, topicName, err := provider.validatedTopicStoragePath(path)
	if err != nil {
		return err
	}
	if err := provider.injectedFault(CleanupOperation, topicName); err != nil {
		return err
	}
	return provider.DiskManager.RemoveTopicStorage(target)
}

func (provider *StorageProvider) validatedTopicStoragePath(path string) (string, string, error) {
	target, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve topic storage path: %w", err)
	}
	target = filepath.Clean(target)
	if filepath.Dir(target) != provider.logRoot {
		return "", "", fmt.Errorf("refusing e2e cleanup outside direct log-root child: %s", target)
	}
	topicName := filepath.Base(target)
	if err := topic.ValidateName(topicName); err != nil {
		return "", "", fmt.Errorf("invalid cleanup topic: %w", err)
	}
	return target, topicName, nil
}

func (provider *StorageProvider) injectedFault(operation, topicName string) error {
	if err := topic.ValidateName(topicName); err != nil {
		return fmt.Errorf("invalid fault topic: %w", err)
	}
	sentinel := filepath.Join(provider.sentinelRoot, operation, topicName)
	_, err := os.Stat(sentinel)
	switch {
	case err == nil:
		return fmt.Errorf("injected e2e storage fault: operation=%s topic=%s", operation, topicName)
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("inspect e2e fault sentinel %s: %w", sentinel, err)
	}
}
