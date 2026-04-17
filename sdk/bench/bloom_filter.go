package bench

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync/atomic"
)

func encodeOffset(partition int, offset int64) []byte {
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(partition))
	binary.BigEndian.PutUint64(buf[4:12], uint64(offset))
	return buf[:]
}

func encodeMessageID(partition int, producerID string, seqNum uint64) []byte {
	idLen := len(producerID)
	buf := make([]byte, 4+idLen+8)
	binary.BigEndian.PutUint32(buf[:4], uint32(partition))
	copy(buf[4:4+idLen], []byte(producerID))
	binary.BigEndian.PutUint64(buf[4+idLen:], seqNum)
	return buf
}

// BloomFilter is a lock-free probabilistic set membership structure.
type BloomFilter struct {
	bits []uint64
	m    uint64
	k    uint64
}

// NewBloomFilter creates a bloom filter sized for expected elements and target false-positive rate.
func NewBloomFilter(expected uint64, fpRate float64) *BloomFilter {
	if expected == 0 {
		expected = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.001
	}

	m := uint64(-1 * float64(expected) * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	if m < 64 {
		m = 64
	}

	k := uint64(float64(m) / float64(expected) * math.Ln2)
	if k < 1 {
		k = 1
	}

	size := (m + 63) / 64
	return &BloomFilter{
		bits: make([]uint64, size),
		m:    m,
		k:    k,
	}
}

func hashf(data []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(data)
	sum1 := h1.Sum64()

	// Use a different seed/type of hash for better independence
	h2 := fnv.New64()
	h2.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	h2.Write(data)
	sum2 := h2.Sum64()

	return sum1, sum2
}

// Add inserts data and returns true if it was already present (probable duplicate).
func (bf *BloomFilter) Add(data []byte) bool {
	if bf.m == 0 {
		return false
	}

	h1, h2 := hashf(data)
	seen := true
	for i := uint64(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % bf.m
		word, bit := idx/64, uint64(1)<<(idx%64)
		old := atomic.OrUint64(&bf.bits[word], bit)
		if old&bit == 0 {
			seen = false
		}
	}
	return seen
}
