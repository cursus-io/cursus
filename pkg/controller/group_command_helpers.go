package controller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

var (
	topicPatternMu           sync.RWMutex
	topicPatternCache        = make(map[string]*regexp.Regexp)
	maxTopicPatternCacheSize = 1024
)

func (ch *CommandHandler) resolveGroupOffsetTopic(groupName, topicName string) (string, string) {
	if ch.Coordinator == nil {
		return topicName, ""
	}
	group := ch.Coordinator.GetGroup(groupName)
	if group == nil {
		return topicName, ""
	}
	if len(group.Topics) > 0 {
		for _, subscribedTopic := range group.Topics {
			if subscribedTopic == topicName {
				return topicName, ""
			}
		}
		expected := strings.Join(group.Topics, ",")
		if group.TopicPattern != "" {
			expected = group.TopicPattern
		}
		return "", fmt.Sprintf("ERROR: topic_not_assigned_to_group expected=%s actual=%s", expected, topicName)
	}
	offsetTopic, ok := resolveOffsetTopic(group.TopicName, topicName)
	if !ok {
		return "", fmt.Sprintf("ERROR: topic_not_assigned_to_group expected=%s actual=%s", group.TopicName, topicName)
	}
	return offsetTopic, ""
}

func resolveOffsetTopic(groupTopic, requestedTopic string) (string, bool) {
	if groupTopic == requestedTopic {
		return requestedTopic, true
	}
	if isTopicMatched(groupTopic, requestedTopic) {
		return requestedTopic, true
	}
	if isTopicMatched(requestedTopic, groupTopic) {
		return groupTopic, true
	}
	return "", false
}

func formatReplicatedGroupError(err error, fallbackCode string) string {
	msg := err.Error()
	if idx := strings.Index(msg, "ERROR:"); idx >= 0 {
		return msg[idx:]
	}
	return fmt.Sprintf("ERROR: %s reason=%q", fallbackCode, msg)
}

func formatCoordinatorError(err error) string {
	if err == nil {
		return "OK"
	}
	msg := err.Error()
	if protocol.IsErrorResponse(msg) {
		return msg
	}
	if strings.Contains(msg, "offset regression") {
		return fmt.Sprintf("ERROR: offset_regression reason=%q", msg)
	}
	if strings.Contains(msg, "not found") {
		return fmt.Sprintf("ERROR: group_not_found reason=%q", msg)
	}
	return fmt.Sprintf("ERROR: coordinator_error reason=%q", msg)
}

// resolveOffset determines the starting offset for a consumer.
func (ch *CommandHandler) resolveOffset(p *topic.Partition, topicName string, cArgs CommonArgs) (uint64, error) {
	savedOffset, found, err := ch.stableGroupOffset(cArgs.GroupName, topicName, cArgs.PartitionID)
	if err != nil {
		return 0, err
	}
	if found {
		return savedOffset, nil
	}
	return resetConsumerOffset(p, cArgs), nil
}

func resetConsumerOffset(p *topic.Partition, cArgs CommonArgs) uint64 {
	if cArgs.HasOffset {
		util.Debug("Using explicitly requested offset %d", cArgs.Offset)
		return cArgs.Offset
	}

	if cArgs.AutoOffsetReset == "latest" {
		latest := p.OffsetRange().Latest
		util.Debug("Reset policy 'latest': starting at %d", latest)
		return latest
	}

	util.Debug("Reset policy 'earliest': starting at 0")
	return 0
}

// stableGroupOffset consults the owner of the group's metadata, which need not
// be the input partition leader. A follower's local offset view is insufficient
// to decide whether an input has a pending transaction reservation.
func (ch *CommandHandler) stableGroupOffset(groupName, topicName string, partition int) (uint64, bool, error) {
	if ch.Config != nil && ch.Config.EnabledDistribution {
		if ch.Cluster == nil || ch.Cluster.Router == nil {
			return 0, false, fmt.Errorf("ERROR: coordinator_not_available")
		}
		owner, _, err := ch.Cluster.Router.FindCoordinator(groupName)
		if err != nil {
			return 0, false, fmt.Errorf("%s", coordinatorUnavailableResponse)
		}
		if owner != ch.Cluster.Router.BrokerID() {
			cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s include_found=true", topicName, partition, groupName)
			response, err := ch.Cluster.Router.ForwardToCoordinator(groupName, cmd)
			if err != nil {
				return 0, false, fmt.Errorf("ERROR: coordinator_not_available reason=%q", err.Error())
			}
			return parseStableOffsetResponse(response)
		}
		if _, local, err := ch.checkCoordinator(groupName); err != nil || !local {
			return 0, false, fmt.Errorf("%s", coordinatorUnavailableResponse)
		}
	}
	if ch.Coordinator == nil {
		if ch.Config != nil && ch.Config.EnabledDistribution {
			return 0, false, fmt.Errorf("ERROR: coordinator_not_available")
		}
		return 0, false, nil
	}
	return ch.Coordinator.GetStableOffset(groupName, topicName, partition)
}

func parseStableOffsetResponse(response string) (uint64, bool, error) {
	if parsed, ok := protocol.ParseErrorResponse(response); ok {
		if parsed.Code == "group_not_found" {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("%s", response)
	}
	if !strings.HasPrefix(response, "OK ") {
		return 0, false, fmt.Errorf("ERROR: coordinator_not_available reason=%q", "invalid stable offset response")
	}
	fields := parseKeyValueArgs(strings.TrimPrefix(response, "OK "))
	offset, offsetErr := strconv.ParseUint(fields["offset"], 10, 64)
	found, foundErr := strconv.ParseBool(fields["found"])
	if offsetErr != nil || foundErr != nil || (!found && offset != 0) {
		return 0, false, fmt.Errorf("ERROR: coordinator_not_available reason=%q", "coordinator lacks a valid stable offset response")
	}
	return offset, found, nil
}

func (ch *CommandHandler) ValidateOwnership(groupName, memberID string, generation int, partition int) bool {
	return ch.ValidateOwnershipFailure(groupName, memberID, generation, partition) == ""
}

func (ch *CommandHandler) ValidateOwnershipFailure(groupName, memberID string, generation int, partition int) string {
	if ch.Coordinator == nil {
		util.Debug("failed to validate ownership: Coordinator is nil.")
		return "ERROR: coordinator_not_available"
	}
	return ch.Coordinator.ValidateOwnershipFailure(groupName, memberID, generation, partition)
}

func isTopicMatched(pattern, topicName string) bool {
	if pattern == topicName {
		return true
	}
	if strings.ContainsAny(pattern, "*?") {
		return matchTopicPattern(pattern, topicName)
	}
	return false
}

func matchTopicPattern(pattern, topicName string) bool {
	topicPatternMu.RLock()
	cached, ok := topicPatternCache[pattern]
	topicPatternMu.RUnlock()
	if ok {
		return cached.MatchString(topicName)
	}

	escaped := regexp.QuoteMeta(pattern)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")
	compiled, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		util.Error("Regex compile error for pattern %s: %v", pattern, err)
		return false
	}

	topicPatternMu.Lock()
	if len(topicPatternCache) >= maxTopicPatternCacheSize {
		topicPatternCache = make(map[string]*regexp.Regexp)
	}
	topicPatternCache[pattern] = compiled
	topicPatternMu.Unlock()
	return compiled.MatchString(topicName)
}
