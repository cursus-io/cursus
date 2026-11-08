package e2e_cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCompoundScheduleAndBounds requires complete fault coverage and finite runs.
func TestCompoundScheduleAndBounds(t *testing.T) {
	for _, seed := range []int64{-1, 0, 1, 3271337} {
		steps := compoundSchedule(seed, 12)
		require.Equal(t, steps, compoundSchedule(seed, 12))
		for offset := 0; offset < len(steps); offset += 3 {
			seen := make(map[string]bool)
			for _, step := range steps[offset : offset+3] {
				seen[step.Fault] = true
				require.Contains(t, []int{0, 1}, step.Peer)
				require.GreaterOrEqual(t, step.QuorumHoldSecs, 2)
				require.LessOrEqual(t, step.QuorumHoldSecs, 5)
			}
			require.Len(t, seen, 3, "every batch must cover all compound faults")
		}
	}
	for _, entry := range []struct{ key, value string }{
		{"CURSUS_COMPOUND_SEED", "unknown"},
		{"CURSUS_COMPOUND_ROUNDS", "2"}, {"CURSUS_COMPOUND_ROUNDS", "2001"},
		{"CURSUS_COMPOUND_DURATION", "25h"}, {"CURSUS_COMPOUND_DURATION", "-1s"},
		{"CURSUS_COMPOUND_DURATION", "forever"},
	} {
		_, err := readCompoundConfig(func(key string) string {
			if key == entry.key {
				return entry.value
			}
			return ""
		})
		require.Error(t, err, "%+v", entry)
	}
	cfg, err := readCompoundConfig(func(key string) string {
		if key == "CURSUS_COMPOUND_DURATION" {
			return "24h"
		}
		return ""
	})
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, cfg.Duration)
}

// TestAckFaultConcurrentJournal detects missing, duplicate and interleaved events.
func TestAckFaultConcurrentJournal(t *testing.T) {
	journal := newAckFaultJournal(t)
	journal.compact = true
	errors := make(chan error, 8)
	var writers sync.WaitGroup
	for writer := 0; writer < 8; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for item := 0; item < 16; item++ {
				if err := journal.recordEvent("invoke", fmt.Sprintf("%d/%d", writer, item)); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	writers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	file, err := os.Open(journal.file.Name())
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	seen := make(map[string]bool)
	for scanner.Scan() {
		var event struct {
			Kind  string `json:"kind"`
			Value string `json:"value"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
		require.Equal(t, "invoke", event.Kind)
		require.False(t, seen[event.Value], "duplicate or interleaved journal event")
		seen[event.Value] = true
	}
	require.NoError(t, scanner.Err())
	require.Len(t, seen, 128)
}

// TestAckFaultJournalErrorsReturnToOwner checks that evidence failures and bounds
// return to the controlling goroutine without sending an unrecorded request.
func TestAckFaultJournalErrorsReturnToOwner(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "journal")
	require.NoError(t, err)
	journal := &ackFaultJournal{t: t, file: file, compact: true}
	require.Error(t, journal.recordEvent("invalid", make(chan int)))
	require.NoError(t, file.Close())
	result := make(chan error, 1)
	go func() { _, _, err := journal.publishAttempt("unused", "topic", ackFaultMessage("p", 1)); result <- err }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, os.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("background journal failure did not return to its owner")
	}
	journal.maxAttempts = 1
	journal.startedAttempts = 1
	_, sent, err := journal.publishAttempt("unused", "topic", ackFaultMessage("p", 1))
	require.ErrorContains(t, err, "attempt limit")
	require.False(t, sent)
}
