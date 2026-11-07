package e2e_cluster

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWriteHistoryOracle(t *testing.T) {
	first := types.Message{ProducerID: "p", SeqNum: 1, Epoch: 2, Payload: "first", Offset: 0}
	second := types.Message{ProducerID: "p", SeqNum: 2, Epoch: 2, Payload: "second", Offset: 1}
	attempt := func(message types.Message, outcome string) writeAttempt {
		return writeAttempt{ID: writeIdentity(message), Message: message,
			PayloadHash: writePayloadHash(message.Payload), Outcome: outcome, AckOffset: message.Offset}
	}
	base := []writeAttempt{attempt(first, "acknowledged"), attempt(second, "acknowledged")}
	t.Run("unknown_then_success_is_one_record", func(t *testing.T) {
		history := append([]writeAttempt{attempt(first, "unknown")}, base...)
		require.NoError(t, checkWriteHistory(history, []types.Message{first, second}))
	})
	for _, outcome := range []string{"unknown", "broker_error"} {
		t.Run(outcome+"_does_not_prove_absence", func(t *testing.T) {
			history := []writeAttempt{base[0], attempt(second, outcome)}
			require.NoError(t, checkWriteHistory(history, []types.Message{first}))
			require.NoError(t, checkWriteHistory(history, []types.Message{first, second}))
		})
	}
	for _, test := range []struct {
		name   string
		mutate func([]types.Message) []types.Message
	}{
		{"missing", func(r []types.Message) []types.Message { return r[:1] }},
		{"duplicate", func(r []types.Message) []types.Message { r[1] = r[0]; r[1].Offset = 1; return r }},
		{"payload", func(r []types.Message) []types.Message { r[1].Payload = "corrupt"; return r }},
		{"offset_collision", func(r []types.Message) []types.Message { r[1].Offset = 0; return r }},
		{"offset_gap", func(r []types.Message) []types.Message { r[1].Offset = 2; return r }},
		{"reorder", func(r []types.Message) []types.Message { return []types.Message{r[1], r[0]} }},
		{"unexpected", func(r []types.Message) []types.Message { r[1].ProducerID = "other"; return r }},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, checkWriteHistory(base, test.mutate([]types.Message{first, second})))
		})
	}
	t.Run("changed_retry_payload", func(t *testing.T) {
		changed := first
		changed.Payload = "replacement"
		history := append([]writeAttempt{attempt(changed, "unknown")}, base...)
		require.ErrorContains(t, checkWriteHistory(history, []types.Message{first, second}), "retry changed payload")
	})
	t.Run("changed_retry_offset", func(t *testing.T) {
		retry := base[0]
		retry.AckOffset = 2
		require.ErrorContains(t, checkWriteHistory(append(base, retry), []types.Message{first, second}), "retry changed acknowledged offset")
	})
	t.Run("reordered_unknown_sequences", func(t *testing.T) {
		third := types.Message{ProducerID: "p", SeqNum: 3, Epoch: 2, Payload: "third", Offset: 1}
		second.Offset = 2
		history := []writeAttempt{base[0], attempt(second, "unknown"), attempt(third, "unknown")}
		require.ErrorContains(t, checkWriteHistory(history, []types.Message{first, third, second}), "sequence reordered")
	})
	t.Run("no_evidence", func(t *testing.T) {
		require.Error(t, checkWriteHistory(nil, nil))
		require.Error(t, checkWriteHistory([]writeAttempt{attempt(first, "unknown")}, nil))
	})
}
