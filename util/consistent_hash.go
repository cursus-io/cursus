package util

import (
	"fmt"
	"hash/crc32"
	"sort"
)

type Hash func(data []byte) uint32

type ConsistentHashRing struct {
	hash     Hash
	replicas int
	keys     []int // Sorted
	hashMap  map[int]string
}

func NewConsistentHashRing(replicas int, fn Hash) *ConsistentHashRing {
	m := &ConsistentHashRing{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *ConsistentHashRing) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *ConsistentHashRing) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(fmt.Sprintf("%s:%d", key, i))))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *ConsistentHashRing) Remove(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(fmt.Sprintf("%s:%d", key, i))))
		delete(m.hashMap, hash)
	}

	// Rebuild sorted keys from remaining hashMap entries
	m.keys = make([]int, 0, len(m.hashMap))
	for h := range m.hashMap {
		m.keys = append(m.keys, h)
	}
	sort.Ints(m.keys)
}

func (m *ConsistentHashRing) Members() int {
	seen := make(map[string]struct{})
	for _, v := range m.hashMap {
		seen[v] = struct{}{}
	}
	return len(seen)
}

func (m *ConsistentHashRing) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}

// GetN returns up to n distinct nodes starting from the key's position on the ring.
// The first element is the same node returned by Get(key).
func (m *ConsistentHashRing) GetN(key string, n int) []string {
	if m.IsEmpty() {
		return nil
	}

	members := m.Members()
	if n > members {
		n = members
	}

	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	var result []string
	seen := make(map[string]struct{})
	for len(result) < n {
		node := m.hashMap[m.keys[idx%len(m.keys)]]
		if _, ok := seen[node]; !ok {
			result = append(result, node)
			seen[node] = struct{}{}
		}
		idx++
	}
	return result
}
