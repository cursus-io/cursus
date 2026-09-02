package controller

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

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
	if strings.HasPrefix(msg, "ERROR:") {
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
	if ch.Coordinator != nil {
		savedOffset, found := ch.Coordinator.GetOffset(cArgs.GroupName, topicName, cArgs.PartitionID)
		if found {
			return savedOffset, nil
		}
	}

	if cArgs.HasOffset {
		util.Debug("Using explicitly requested offset %d", cArgs.Offset)
		return cArgs.Offset, nil
	}

	if cArgs.AutoOffsetReset == "latest" {
		latest := p.OffsetRange().Latest
		util.Debug("Reset policy 'latest': starting at %d", latest)
		return latest, nil
	}

	util.Debug("Reset policy 'earliest': starting at 0")
	return 0, nil
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
