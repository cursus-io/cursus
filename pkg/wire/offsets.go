package wire

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// MaxOffsetPairs bounds one Wire v2 offset-list field.
const MaxOffsetPairs = 1024

// OffsetPair is one partition's next offset in an offsets= field.
type OffsetPair struct {
	Partition int
	Offset    uint64
}

// EncodeOffsetPairs returns the canonical, partition-sorted Wire v2 value used
// by BATCH_COMMIT and SEND_OFFSETS_TO_TXN.
func EncodeOffsetPairs(pairs []OffsetPair) (string, error) {
	if len(pairs) == 0 {
		return "", fmt.Errorf("missing offset pairs")
	}
	if len(pairs) > MaxOffsetPairs {
		return "", fmt.Errorf("offset pair count %d exceeds maximum %d", len(pairs), MaxOffsetPairs)
	}
	ordered := append([]OffsetPair(nil), pairs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Partition < ordered[j].Partition })
	encoded := make([]string, 0, len(ordered))
	lastPartition := -1
	for _, pair := range ordered {
		if pair.Partition < 0 {
			return "", fmt.Errorf("invalid partition %d", pair.Partition)
		}
		if pair.Partition == lastPartition {
			return "", fmt.Errorf("duplicate partition %d", pair.Partition)
		}
		lastPartition = pair.Partition
		encoded = append(encoded, fmt.Sprintf("P%d:%d", pair.Partition, pair.Offset))
	}
	return strings.Join(encoded, ","), nil
}

// DecodeOffsetPairs parses the current Wire v2 offsets= value. Positional
// offset lists are intentionally unsupported.
func DecodeOffsetPairs(value string) ([]OffsetPair, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("missing offset pairs")
	}
	commaCount := strings.Count(value, ",")
	if commaCount >= MaxOffsetPairs {
		return nil, fmt.Errorf("offset pair count exceeds maximum %d", MaxOffsetPairs)
	}
	rawPairs := strings.Split(value, ",")
	pairs := make([]OffsetPair, 0, len(rawPairs))
	seen := make(map[int]struct{}, len(rawPairs))
	for _, rawPair := range rawPairs {
		parts := strings.SplitN(rawPair, ":", 2)
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "P") {
			return nil, fmt.Errorf("invalid offset pair %q", rawPair)
		}
		partition, err := strconv.Atoi(strings.TrimPrefix(parts[0], "P"))
		if err != nil || partition < 0 {
			return nil, fmt.Errorf("invalid partition in offset pair %q", rawPair)
		}
		offset, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid offset in pair %q", rawPair)
		}
		if _, duplicate := seen[partition]; duplicate {
			return nil, fmt.Errorf("duplicate partition %d", partition)
		}
		seen[partition] = struct{}{}
		pairs = append(pairs, OffsetPair{Partition: partition, Offset: offset})
	}
	return pairs, nil
}
