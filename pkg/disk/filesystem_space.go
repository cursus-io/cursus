package disk

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const filesystemSpaceCacheTTL = time.Second

func checkedFilesystemBytes(blocks, blockSize uint64) (uint64, error) {
	if blockSize != 0 && blocks > math.MaxUint64/blockSize {
		return 0, fmt.Errorf("block count overflow: blocks=%d size=%d", blocks, blockSize)
	}
	return blocks * blockSize, nil
}

type filesystemSpaceSnapshot struct {
	Free  uint64
	Total uint64
	Ready bool
}

type diskHeadroomGuard struct {
	mu             sync.Mutex
	path           string
	minFreeBytes   uint64
	minFreePercent float64
	checkedAt      time.Time
	free           uint64
	total          uint64
	reserved       uint64
	lastErr        error
}

func newDiskHeadroomGuard(path string, minFreeBytes int64, minFreePercent float64) *diskHeadroomGuard {
	if minFreeBytes < 0 {
		minFreeBytes = 0
	}
	if minFreePercent < 0 {
		minFreePercent = 0
	}
	return &diskHeadroomGuard{
		path:           path,
		minFreeBytes:   uint64(minFreeBytes),
		minFreePercent: minFreePercent,
	}
}

func (g *diskHeadroomGuard) reserve(required uint64, forceRefresh bool) (filesystemSpaceSnapshot, error) {
	if g == nil {
		return filesystemSpaceSnapshot{Ready: true}, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	if forceRefresh || g.checkedAt.IsZero() || now.Sub(g.checkedAt) >= filesystemSpaceCacheTTL {
		free, total, err := queryFilesystemSpace(g.path)
		g.checkedAt = now
		g.reserved = 0
		g.free = free
		g.total = total
		g.lastErr = err
	}

	snapshot := filesystemSpaceSnapshot{Free: g.free, Total: g.total}
	if g.lastErr != nil {
		return snapshot, fmt.Errorf("inspect filesystem capacity for %s: %w", g.path, g.lastErr)
	}
	available := g.free
	if g.reserved >= available {
		available = 0
	} else {
		available -= g.reserved
	}
	remaining := available
	if required >= remaining {
		remaining = 0
	} else {
		remaining -= required
	}

	minimum := g.minFreeBytes
	if g.total > 0 && g.minFreePercent > 0 {
		percentageMinimum := uint64(math.Ceil(float64(g.total) * g.minFreePercent / 100))
		if percentageMinimum > minimum {
			minimum = percentageMinimum
		}
	}
	if remaining < minimum {
		return snapshot, fmt.Errorf("insufficient filesystem headroom: free=%d reserved=%d write=%d remaining=%d required=%d", g.free, g.reserved, required, remaining, minimum)
	}
	if math.MaxUint64-g.reserved < required {
		return snapshot, fmt.Errorf("filesystem headroom reservation overflow: reserved=%d write=%d", g.reserved, required)
	}
	g.reserved += required
	snapshot.Ready = true
	return snapshot, nil
}

func (g *diskHeadroomGuard) snapshot(forceRefresh bool) (filesystemSpaceSnapshot, error) {
	return g.reserve(0, forceRefresh)
}
