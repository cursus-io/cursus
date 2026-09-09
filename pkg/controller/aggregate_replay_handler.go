package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/aggregate"
	"github.com/cursus-io/cursus/pkg/types"
)

func (ch *CommandHandler) aggregateReplayTarget(cmd, prefix string) (string, int, string, string) {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, prefix))
	topicName, matchID := args["topic"], firstNonEmpty(args["match_id"], args["key"])
	if topicName == "" {
		return "", 0, "", "ERROR: missing_topic"
	}
	if matchID == "" {
		return "", 0, "", "ERROR: missing_match_id"
	}
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return "", 0, "", fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.Policy.AggregateReplay {
		return "", 0, "", fmt.Sprintf("ERROR: aggregate_replay_not_enabled topic=%s", topicName)
	}
	partition := t.GetPartitionForMessage(typesMessageForKey(matchID))
	if partition < 0 {
		return "", 0, "", "ERROR: no_partitions_available"
	}
	if supplied := args["partition"]; supplied != "" {
		got, err := strconv.Atoi(supplied)
		if err != nil || got != partition {
			return "", 0, "", fmt.Sprintf("ERROR: aggregate_partition_mismatch expected=%d got=%s", partition, supplied)
		}
	}
	return topicName, partition, matchID, ""
}

// typesMessageForKey keeps aggregate routing tied to the broker's existing key hash.
func typesMessageForKey(key string) types.Message { return types.Message{Key: key} }

func (ch *CommandHandler) handleAggregateReplayProof(cmd string) string {
	topicName, partition, matchID, errResp := ch.aggregateReplayTarget(cmd, "AGGREGATE_REPLAY_PROOF ")
	if errResp != "" {
		return errResp
	}
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "AGGREGATE_REPLAY_PROOF "))
	var expected *uint64
	if raw := args["expected_last_sequence"]; raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || value == 0 {
			return "ERROR: invalid_expected_last_sequence"
		}
		expected = &value
	}
	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
	}
	proof, _ := aggregate.Inspect(ch.TopicManager.GetTopic(topicName), partition, matchID, expected)
	encoded, err := json.Marshal(proof)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_aggregate_proof_failed reason=%q", err.Error())
	}
	if proof.Status != aggregate.StatusComplete {
		return fmt.Sprintf("ERROR: aggregate_proof_%s proof=%s", proof.Status, string(encoded))
	}
	return "OK proof=" + string(encoded)
}

func (ch *CommandHandler) handleAggregateEventRangeRead(cmd string) string {
	topicName, partition, matchID, errResp := ch.aggregateReplayTarget(cmd, "AGGREGATE_EVENT_RANGE_READ ")
	if errResp != "" {
		return errResp
	}
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "AGGREGATE_EVENT_RANGE_READ "))
	from, err := strconv.ParseUint(args["from_sequence"], 10, 64)
	if err != nil || from == 0 {
		return "ERROR: invalid_from_sequence"
	}
	to, err := strconv.ParseUint(args["to_sequence"], 10, 64)
	if err != nil || to < from {
		return "ERROR: invalid_to_sequence"
	}
	max := 1000
	if raw := args["max_records"]; raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			return "ERROR: invalid_max_records"
		}
		max = parsed
	}
	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
	}
	proof, events := aggregate.Inspect(ch.TopicManager.GetTopic(topicName), partition, matchID, nil)
	if proof.Status != aggregate.StatusComplete {
		encoded, _ := json.Marshal(proof)
		return fmt.Sprintf("ERROR: aggregate_range_%s proof=%s", proof.Status, string(encoded))
	}
	filtered := make([]aggregate.Event, 0)
	for _, event := range events {
		if event.EventSequence >= from && event.EventSequence <= to {
			filtered = append(filtered, event)
			if len(filtered) == max {
				break
			}
		}
	}
	if uint64(len(filtered)) != minUint64(to-from+1, uint64(max)) {
		return fmt.Sprintf("ERROR: aggregate_range_incomplete from=%d to=%d returned=%d", from, to, len(filtered))
	}
	response := struct {
		Proof  aggregate.Proof   `json:"proof"`
		Events []aggregate.Event `json:"events"`
	}{proof, filtered}
	encoded, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		return fmt.Sprintf("ERROR: marshal_aggregate_range_failed reason=%q", marshalErr.Error())
	}
	return "OK range=" + string(encoded)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
