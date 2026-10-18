package eventsource

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

// aggregateCommand builds a replay append with a per-aggregate producer.
func aggregateCommand(key, id string, version int) string {
	return fmt.Sprintf("APPEND_STREAM topic=matches key=%s version=%d event_id=%s producerId=%s seqNum=%d message=payload", key, version, id, key, version)
}

// TestAggregateEventIndexScansOnlyNewCommittedRecords guards incremental reads,
// same-version retries and cross-key identity conflict rejection.
func TestAggregateEventIndexScansOnlyNewCommittedRecords(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { require.NoError(t, h.Close()) }()
	require.NoError(t, h.tm.CreateTopicWithPolicy("matches", 1, true, true, topic.Policy{AggregateReplay: true}))
	for i := 1; i <= 100; i++ {
		require.True(t, strings.HasPrefix(h.HandleAppendStream(aggregateCommand("match", fmt.Sprint(i), i)), "OK"))
	}
	storage := hp.handlers["matches:0"]
	var offsets []uint64
	storage.beforeRead = func(off uint64) { offsets = append(offsets, off) }
	for i := 101; i <= 105; i++ {
		require.True(t, strings.HasPrefix(h.HandleAppendStream(aggregateCommand("match", fmt.Sprint(i), i)), "OK"))
	}
	require.Equal(t, []uint64{99, 100, 101, 102, 103}, offsets, "warm uniqueness checks must not rescan the retained prefix")
	storage.beforeRead = nil
	require.Contains(t, h.HandleAppendStream(aggregateCommand("other", "1", 1)), "aggregate_identity_conflict")
	require.Contains(t, h.HandleAppendStream(aggregateCommand("match", "105", 105)), "OK version=105 offset=104")
	require.Contains(t, h.HandleAppendStream(aggregateCommand("match", "different", 105)), "aggregate_identity_conflict")
	require.Len(t, storage.msgs, 105)
}

// TestAggregateEventIndexSerializesCrossKeyDuplicates permits only one append
// when concurrent writers reuse an ID across different aggregate keys.
func TestAggregateEventIndexSerializesCrossKeyDuplicates(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { require.NoError(t, h.Close()) }()
	require.NoError(t, h.tm.CreateTopicWithPolicy("matches", 1, true, true, topic.Policy{AggregateReplay: true}))
	// Initialize lazy topic/index state before concurrent calls.
	_, err := h.getIndex("matches", 0)
	require.NoError(t, err)
	responses := make(chan string, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses <- h.HandleAppendStream(aggregateCommand(fmt.Sprint(i), "shared-id", 1))
		}(i)
	}
	wg.Wait()
	close(responses)
	successes := 0
	for response := range responses {
		if strings.HasPrefix(response, "OK") {
			successes++
		} else {
			require.Contains(t, response, "aggregate_identity_conflict")
		}
	}
	require.Equal(t, 1, successes)
	require.Len(t, hp.handlers["matches:0"].msgs, 1)
}
