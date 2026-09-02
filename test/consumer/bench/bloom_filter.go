package bench

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync/atomic"
)

func encodeOffset(partition int, offset int64) []byte {
	var buf [2 * binary.MaxVarintLen64]byte
	length := binary.PutVarint(buf[:], int64(partition))
	length += binary.PutVarint(buf[length:], offset)
	return buf[:length]
}

func encodeMessageID(partition int, producerID string, seqNum uint64) []byte {
	var partitionBytes [binary.MaxVarintLen64]byte
	partitionLength := binary.PutVarint(partitionBytes[:], int64(partition))
	buf := make([]byte, partitionLength+len(producerID)+8)
	copy(buf, partitionBytes[:partitionLength])
	copy(buf[partitionLength:partitionLength+len(producerID)], producerID)
	binary.BigEndian.PutUint64(buf[partitionLength+len(producerID):], seqNum)
	return buf
}

type BloomFilter struct {
	bits []uint64
	m    uint64
	k    uint64
}

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
	_, _ = h1.Write(data)
	sum1 := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte{0x9e, 0x37, 0x79, 0xb9})
	_, _ = h2.Write(data)
	sum2 := h2.Sum64()

	return sum1, sum2
}

func (bf *BloomFilter) Add(data []byte) bool {
	if bf.m == 0 {
		return false
	}

	h1, h2 := hashf(data)

	var seen = true
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
