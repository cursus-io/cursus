package disk

import (
	"fmt"
	"os"
	"sync"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type DiskManager struct {
	mu       sync.Mutex
	handlers map[string]*DiskHandler
	cfg      *config.Config
}

func NewDiskManager(cfg *config.Config) *DiskManager {
	return &DiskManager{
		handlers: make(map[string]*DiskHandler),
		cfg:      cfg,
	}
}

// GetHandler returns a StorageHandler for a given name or creates one if missing
func (dm *DiskManager) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := fmt.Sprintf("%s_%d", topic, partitionID)
	if dh, ok := dm.handlers[key]; ok {
		return dh, nil
	}

	if err := os.MkdirAll(dm.cfg.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", dm.cfg.LogDir, err)
	}

	dh, err := NewDiskHandler(dm.cfg, topic, partitionID)
	if err != nil {
		return nil, err
	}

	dm.handlers[key] = dh
	return dh, nil
}

// CloseAllHandlers should be implemented to ensure all DiskHandlers are closed properly
func (dm *DiskManager) CloseAllHandlers() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for name, dh := range dm.handlers {
		util.Debug("Closing DiskHandler for %s", name)
		if err := dh.Close(); err != nil {
			util.Warn("Failed to close DiskHandler for %s: %v", name, err)
		}
		delete(dm.handlers, name)
	}
}
