// +build ignore

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DiskMessage mirrors types.DiskMessage for standalone verification.
type DiskMessage struct {
	Topic      string
	Partition  int32
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Epoch      int64
	Payload    string
}

func deserializeDiskMessage(data []byte) (DiskMessage, error) {
	var msg DiskMessage
	pos := 0

	if pos+2 > len(data) {
		return msg, fmt.Errorf("too short for topic length")
	}
	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if pos+topicLen > len(data) {
		return msg, fmt.Errorf("too short for topic")
	}
	msg.Topic = string(data[pos : pos+topicLen])
	pos += topicLen

	if pos+4 > len(data) {
		return msg, fmt.Errorf("too short for partition")
	}
	msg.Partition = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if pos+8 > len(data) {
		return msg, fmt.Errorf("too short for offset")
	}
	msg.Offset = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	if pos+2 > len(data) {
		return msg, fmt.Errorf("too short for producerID length")
	}
	prodLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if pos+prodLen > len(data) {
		return msg, fmt.Errorf("too short for producerID")
	}
	msg.ProducerID = string(data[pos : pos+prodLen])
	pos += prodLen

	if pos+8 > len(data) {
		return msg, fmt.Errorf("too short for seqNum")
	}
	msg.SeqNum = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	if pos+8 > len(data) {
		return msg, fmt.Errorf("too short for epoch")
	}
	msg.Epoch = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	if pos+4 > len(data) {
		return msg, fmt.Errorf("too short for payload length")
	}
	payloadLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if pos+payloadLen > len(data) {
		return msg, fmt.Errorf("too short for payload")
	}
	msg.Payload = string(data[pos : pos+payloadLen])

	return msg, nil
}

func readSegmentFile(path string) ([]DiskMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var messages []DiskMessage
	pos := 0
	for pos+4 <= len(data) {
		msgLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if msgLen <= 0 {
			return nil, fmt.Errorf("invalid record length %d at byte %d", msgLen, pos-4)
		}
		if pos+msgLen > len(data) {
			return nil, fmt.Errorf("truncated record at byte %d: need %d bytes but only %d remain", pos, msgLen, len(data)-pos)
		}
		msg, err := deserializeDiskMessage(data[pos : pos+msgLen])
		if err != nil {
			return nil, fmt.Errorf("at byte %d: %w", pos, err)
		}
		messages = append(messages, msg)
		pos += msgLen
	}
	return messages, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <log-dir> [expected-count]\n", os.Args[0])
		os.Exit(1)
	}
	logDir := os.Args[1]
	expectedTotal := -1
	if len(os.Args) >= 3 {
		fmt.Sscanf(os.Args[2], "%d", &expectedTotal)
	}

	// Find all .log segment files
	var segmentFiles []string
	filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(path, ".log") && strings.Contains(path, "segment") {
			segmentFiles = append(segmentFiles, path)
		}
		return nil
	})

	sort.Strings(segmentFiles)

	if len(segmentFiles) == 0 {
		fmt.Printf("ERROR: No segment files found in %s\n", logDir)
		os.Exit(1)
	}

	fmt.Printf("Found %d segment files in %s\n", len(segmentFiles), logDir)

	// Per-partition tracking
	type partitionStats struct {
		count     int
		offsets   map[uint64]int // offset -> count
		minOffset uint64
		maxOffset uint64
	}

	partitions := make(map[int32]*partitionStats)
	totalMessages := 0
	totalDuplicates := 0

	for _, sf := range segmentFiles {
		msgs, err := readSegmentFile(sf)
		if err != nil {
			fmt.Printf("ERROR reading %s: %v\n", sf, err)
			os.Exit(1)
		}

		rel, _ := filepath.Rel(logDir, sf)
		fmt.Printf("  %s: %d messages\n", rel, len(msgs))

		for _, msg := range msgs {
			ps, ok := partitions[msg.Partition]
			if !ok {
				ps = &partitionStats{
					offsets:   make(map[uint64]int),
					minOffset: msg.Offset,
					maxOffset: msg.Offset,
				}
				partitions[msg.Partition] = ps
			}

			ps.offsets[msg.Offset]++
			if ps.offsets[msg.Offset] > 1 {
				totalDuplicates++
				fmt.Printf("  DUPLICATE: partition=%d offset=%d (seen %d times)\n", msg.Partition, msg.Offset, ps.offsets[msg.Offset])
			}
			ps.count++
			if msg.Offset < ps.minOffset {
				ps.minOffset = msg.Offset
			}
			if msg.Offset > ps.maxOffset {
				ps.maxOffset = msg.Offset
			}
			totalMessages++
		}
	}

	fmt.Println("\n========================================")
	fmt.Printf("VERIFICATION RESULTS\n")
	fmt.Printf("Total messages on disk  : %d\n", totalMessages)
	if expectedTotal >= 0 {
		fmt.Printf("Expected messages       : %d\n", expectedTotal)
	}
	fmt.Printf("Total duplicates        : %d\n", totalDuplicates)
	fmt.Printf("Partitions              : %d\n", len(partitions))

	// Sort partition IDs for display
	var pids []int
	for pid := range partitions {
		pids = append(pids, int(pid))
	}
	sort.Ints(pids)

	fmt.Println("\nPer-partition breakdown:")
	allContiguous := true
	for _, pid := range pids {
		ps := partitions[int32(pid)]
		expectedRange := ps.maxOffset - ps.minOffset + 1
		contiguous := int(expectedRange) == ps.count
		status := "OK"
		if !contiguous {
			status = "GAP DETECTED"
			allContiguous = false
		}
		fmt.Printf("  Partition %2d: %d msgs, offsets [%d..%d], %s\n",
			pid, ps.count, ps.minOffset, ps.maxOffset, status)
	}

	fmt.Println("\n========================================")
	countOK := expectedTotal < 0 || totalMessages == expectedTotal
	if countOK && totalDuplicates == 0 && allContiguous {
		fmt.Println("RESULT: PASS - All messages verified, no duplicates, no gaps")
	} else {
		fmt.Println("RESULT: FAIL")
		if !countOK {
			fmt.Printf("  - Message count mismatch: got %d, expected %d\n", totalMessages, expectedTotal)
		}
		if totalDuplicates > 0 {
			fmt.Printf("  - Found %d duplicate offsets\n", totalDuplicates)
		}
		if !allContiguous {
			fmt.Println("  - Offset gaps detected in some partitions")
		}
	}
	fmt.Println("========================================")
}
