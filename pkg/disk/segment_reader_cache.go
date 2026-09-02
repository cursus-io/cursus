package disk

import (
	"fmt"
	"sync"

	"golang.org/x/exp/mmap"
)

const defaultSegmentReaderCacheEntries = 8

type segmentReaderCache struct {
	mu         sync.Mutex
	entries    map[uint64]*segmentReaderEntry
	maxEntries int
	clock      uint64
	closed     bool
	hits       uint64
	misses     uint64
	evictions  uint64
}

type segmentReaderEntry struct {
	base     uint64
	reader   *mmap.ReaderAt
	refs     int
	lastUsed uint64
	stale    bool
}

type segmentReaderLease struct {
	cache    *segmentReaderCache
	entry    *segmentReaderEntry
	reader   *mmap.ReaderAt
	closeMu  sync.Mutex
	closed   bool
	closeErr error
}

type segmentReaderCacheStats struct {
	Entries   int
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

func newSegmentReaderCache(maxEntries int) *segmentReaderCache {
	return &segmentReaderCache{
		entries:    make(map[uint64]*segmentReaderEntry),
		maxEntries: maxEntries,
	}
}

func (c *segmentReaderCache) acquire(base uint64, path string) (*segmentReaderLease, error) {
	if c == nil || c.maxEntries <= 0 {
		reader, err := mmap.Open(path)
		if err != nil {
			return nil, err
		}
		return &segmentReaderLease{reader: reader}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("segment reader cache is closed")
	}
	c.clock++
	if entry, ok := c.entries[base]; ok && !entry.stale {
		entry.refs++
		entry.lastUsed = c.clock
		c.hits++
		return &segmentReaderLease{cache: c, entry: entry, reader: entry.reader}, nil
	}

	c.misses++
	if len(c.entries) >= c.maxEntries && !c.evictOldestIdleLocked() {
		reader, err := mmap.Open(path)
		if err != nil {
			return nil, err
		}
		return &segmentReaderLease{reader: reader}, nil
	}
	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	entry := &segmentReaderEntry{base: base, reader: reader, refs: 1, lastUsed: c.clock}
	c.entries[base] = entry
	return &segmentReaderLease{cache: c, entry: entry, reader: reader}, nil
}

func (c *segmentReaderCache) evictOldestIdleLocked() bool {
	var candidate *segmentReaderEntry
	for _, entry := range c.entries {
		if entry.refs != 0 {
			continue
		}
		if candidate == nil || entry.lastUsed < candidate.lastUsed {
			candidate = entry
		}
	}
	if candidate == nil {
		return false
	}
	delete(c.entries, candidate.base)
	_ = candidate.reader.Close()
	c.evictions++
	return true
}

func (c *segmentReaderCache) release(entry *segmentReaderEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.refs <= 0 {
		return fmt.Errorf("segment reader %d released without a reference", entry.base)
	}
	entry.refs--
	if entry.refs == 0 && (entry.stale || c.closed) {
		delete(c.entries, entry.base)
		return entry.reader.Close()
	}
	return nil
}

func (c *segmentReaderCache) invalidate(base uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[base]
	if !ok {
		return nil
	}
	if entry.refs != 0 {
		return fmt.Errorf("segment %d has %d active cached reader(s)", base, entry.refs)
	}
	delete(c.entries, base)
	return entry.reader.Close()
}

func (c *segmentReaderCache) invalidateAll() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries {
		if entry.refs != 0 {
			return fmt.Errorf("segment %d has %d active cached reader(s)", entry.base, entry.refs)
		}
	}
	var firstErr error
	for base, entry := range c.entries {
		if err := entry.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.entries, base)
	}
	return firstErr
}

func (c *segmentReaderCache) close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var firstErr error
	for base, entry := range c.entries {
		if entry.refs != 0 {
			entry.stale = true
			continue
		}
		if err := entry.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.entries, base)
	}
	return firstErr
}

func (c *segmentReaderCache) stats() segmentReaderCacheStats {
	if c == nil {
		return segmentReaderCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return segmentReaderCacheStats{
		Entries: len(c.entries), Hits: c.hits, Misses: c.misses, Evictions: c.evictions,
	}
}

func (l *segmentReaderLease) Reader() *mmap.ReaderAt {
	if l == nil {
		return nil
	}
	return l.reader
}

func (l *segmentReaderLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeMu.Lock()
	defer l.closeMu.Unlock()
	if l.closed {
		return l.closeErr
	}
	l.closed = true
	if l.cache != nil {
		l.closeErr = l.cache.release(l.entry)
	} else if l.reader != nil {
		l.closeErr = l.reader.Close()
	}
	return l.closeErr
}
