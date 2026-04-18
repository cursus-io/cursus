package controller

import (
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	argsMap := parseKeyValueArgs(rawCmd[8:])
	if err := ch.validateConsumeArgs(argsMap); err != nil {
		return 0, err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return 0, err
	}
	if err := ch.checkLeaderOrRedirect(conn); err != nil {
		if err.Error() == "not leader" {
			return 0, nil
		}
		return 0, err
	}

	matchedTopics, err := ch.matchTopicPattern(cArgs.TopicName)
	if err != nil {
		return 0, err
	}

	if len(matchedTopics) == 0 {
		return 0, fmt.Errorf("no assigned topics match pattern '%s'", cArgs.TopicName)
	}

	totalStreamed := 0
	var allMessages []types.Message

	for _, tName := range matchedTopics {
		if totalStreamed >= cArgs.BatchSize {
			break
		}

		remainingBatch := cArgs.BatchSize - totalStreamed
		messages, err := ch.readFromTopic(tName, cArgs, ctx, remainingBatch)
		if err != nil {
			return totalStreamed, err
		}
		if len(messages) > 0 {
			allMessages = append(allMessages, messages...)
			totalStreamed += len(messages)
		}
	}

	if totalStreamed == 0 && cArgs.WaitTimeout > 0 {
		startTime := time.Now()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for time.Since(startTime) < cArgs.WaitTimeout {
			<-ticker.C
			for _, tName := range matchedTopics {
				messages, err := ch.readFromTopic(tName, cArgs, ctx, cArgs.BatchSize)
				if err != nil {
					return 0, err
				}
				if len(messages) > 0 {
					allMessages = append(allMessages, messages...)
					totalStreamed += len(messages)
					goto sendBatch
				}
			}
		}
	}

sendBatch:
	batchData, err := util.EncodeBatchMessages(cArgs.TopicName, cArgs.PartitionID, "1", false, allMessages)
	if err != nil {
		return 0, fmt.Errorf("failed to encode batch: %w", err)
	}

	if err := util.WriteWithLength(conn, batchData); err != nil {
		return 0, fmt.Errorf("failed to stream batch: %w", err)
	}

	return totalStreamed, nil
}

func (ch *CommandHandler) readFromTopic(topicName string, cArgs CommonArgs, ctx *ClientContext, batchSize int) ([]types.Message, error) {
	if cArgs.MemberID == "" {
		return nil, fmt.Errorf("missing member parameter")
	}

	_, p, err := ch.getTopicAndPartition(topicName, cArgs.PartitionID)
	if err != nil {
		return nil, err
	}

	currentGen := -1
	if ch.Coordinator != nil {
		currentGen = ch.Coordinator.GetGeneration(cArgs.GroupName)

		if err := ch.Coordinator.RecordHeartbeat(cArgs.GroupName, cArgs.MemberID); err != nil {
			util.Warn("Failed to record heartbeat during consume: %v", err)
			return nil, fmt.Errorf("heartbeat failed: %w", err)
		}
	}

	// Use generation from arguments if provided, otherwise fallback to coordinator
	if cArgs.Generation != -1 {
		currentGen = cArgs.Generation
	}

	if ctx.Generation != currentGen {
		ctx.OffsetCache = make(map[string]uint64)
		ctx.Generation = currentGen
		ctx.MemberID = cArgs.MemberID
		util.Debug("Generation changed to %d, cache cleared", currentGen)
	}

	cacheKey := fmt.Sprintf("%s-%d", topicName, cArgs.PartitionID)
	var currentOffset uint64
	if cArgs.HasOffset {
		currentOffset = cArgs.Offset
		util.Debug("Using explicit offset: %d", currentOffset)
	} else if cached, ok := ctx.OffsetCache[cacheKey]; ok {
		currentOffset = cached
	} else {
		actualOffset, err := ch.resolveOffset(p, topicName, cArgs)
		if err != nil {
			return nil, err
		}
		currentOffset = actualOffset
	}

	if !ch.ValidateOwnership(cArgs.GroupName, cArgs.MemberID, currentGen, cArgs.PartitionID) {
		util.Debug("Ownership validation failed for topic %s (gen=%d)", topicName, currentGen)
		return nil, nil // Skip this topic if not owned (allowing regex to continue with other owned topics)
	}

	messages, err := p.ReadCommitted(currentOffset, batchSize)
	if err != nil {
		util.Error("Failed to read messages from topic %s: %v", topicName, err)
		return nil, err
	}

	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		ctx.OffsetCache[cacheKey] = lastMsg.Offset + 1
	}

	return messages, nil
}

func (ch *CommandHandler) matchTopicPattern(pattern string) ([]string, error) {
	const maxPatternLength = 256
	if len(pattern) > maxPatternLength {
		return nil, fmt.Errorf("topic pattern exceeds maximum length of %d characters", maxPatternLength)
	}

	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		if ch.TopicManager.GetTopic(pattern) == nil {
			return nil, fmt.Errorf("topic '%s' does not exist", pattern)
		}
		return []string{pattern}, nil
	}

	escaped := regexp.QuoteMeta(pattern)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")
	regex, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		return nil, fmt.Errorf("invalid topic pattern: %w", err)
	}

	allTopics := ch.TopicManager.ListTopics()
	var matchedTopics []string
	for _, topic := range allTopics {
		if regex.MatchString(topic) {
			matchedTopics = append(matchedTopics, topic)
		}
	}

	sort.Strings(matchedTopics)
	if len(matchedTopics) == 0 {
		return nil, fmt.Errorf("no topics match pattern '%s'", pattern)
	}

	return matchedTopics, nil
}

func (ch *CommandHandler) HandleStreamCommand(conn net.Conn, rawCmd string, ctx *ClientContext) error {
	if len(rawCmd) < 7 {
		return fmt.Errorf("invalid STREAM command format")
	}

	argsMap := parseKeyValueArgs(rawCmd[7:])
	if err := ch.validateStreamArgs(argsMap); err != nil {
		return err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return err
	}
	ctx.ConsumerGroup = cArgs.GroupName

	if err := ch.checkLeaderOrRedirect(conn); err != nil {
		if err.Error() == "not leader" {
			return nil
		}
		return err
	}

	if !ch.ValidateOwnership(ctx.ConsumerGroup, cArgs.MemberID, cArgs.Generation, cArgs.PartitionID) {
		return fmt.Errorf("not partition owner or generation mismatch")
	}

	ctx.Generation = cArgs.Generation
	ctx.MemberID = cArgs.MemberID

	t, p, err := ch.getTopicAndPartition(cArgs.TopicName, cArgs.PartitionID)
	if err != nil {
		return err
	}

	actualOffset, err := ch.resolveOffset(p, cArgs.TopicName, cArgs)
	if err != nil {
		return err
	}

	streamKey := fmt.Sprintf("%s:%d:%s", cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName)
	streamConn := stream.NewStreamConnection(conn, cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName, actualOffset)
	streamConn.SetBatchSize(cArgs.BatchSize)
	streamConn.SetInterval(100 * time.Millisecond)
	if ch.Coordinator != nil {
		streamConn.SetCommitter(ch.Coordinator)
	}

	// Pass partition's message signal for event-driven streaming
	signalCh := t.NewMessageSignal(cArgs.PartitionID)
	if signalCh != nil {
		streamConn.SetNewMessageCh(signalCh)
	}

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return p.ReadCommitted(offset, max)
	}

	return ch.StreamManager.AddStream(streamKey, streamConn, readFn, ch.Config.StreamCommitInterval)
}

func (ch *CommandHandler) validateStreamSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[7:])
	if args["topic"] == "" || args["partition"] == "" || args["group"] == "" {
		return ch.fail(raw, "ERROR: invalid STREAM syntax")
	}
	return STREAM_DATA_SIGNAL
}

func (ch *CommandHandler) validateConsumeSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[8:])
	if args["topic"] == "" || args["partition"] == "" || args["offset"] == "" || args["member"] == "" {
		return ch.fail(raw, "ERROR: invalid CONSUME syntax")
	}
	return STREAM_DATA_SIGNAL
}

// checkLeaderOrRedirect checks if this broker is the leader and writes a redirect error if not.
func (ch *CommandHandler) checkLeaderOrRedirect(conn net.Conn) error {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.RaftManager.IsLeader() {
		return nil
	}

	leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
	if leaderAddr == "" {
		return fmt.Errorf("no leader available")
	}

	serviceLeader := leaderAddr
	if host, _, splitErr := net.SplitHostPort(leaderAddr); splitErr == nil {
		serviceLeader = net.JoinHostPort(host, strconv.Itoa(ch.Config.BrokerPort))
	}

	errResp := fmt.Sprintf("ERROR: NOT_LEADER LEADER_IS %s", serviceLeader)
	util.Warn("leader redirect: %s", errResp)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send leader redirect: %w", err)
	}
	return fmt.Errorf("not leader")
}

func (ch *CommandHandler) getTopicAndPartition(topicName string, partitionID int) (*topic.Topic, *topic.Partition, error) {
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return nil, nil, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, nil, err
	}

	return t, p, nil
}

func (ch *CommandHandler) resolveConsumerGroup(groupName string) string {
	if groupName == "" || groupName == "-" {
		return "default-group"
	}
	return groupName
}

type CommonArgs struct {
	TopicName       string
	PartitionID     int
	GroupName       string
	MemberID        string
	Generation      int
	HasOffset       bool
	Offset          uint64
	BatchSize       int
	WaitTimeout     time.Duration
	AutoOffsetReset string
}

func (ch *CommandHandler) parseCommonArgs(args map[string]string) (CommonArgs, error) {
	pID, err := strconv.Atoi(args["partition"])
	if err != nil && args["partition"] != "" {
		return CommonArgs{}, fmt.Errorf("invalid partition value: %s", args["partition"])
	}

	gen := -1
	genStr := args["generation"]
	if genStr != "" {
		g, err := strconv.Atoi(genStr)
		if err != nil {
			return CommonArgs{}, fmt.Errorf("invalid generation value: %s", genStr)
		}
		gen = g
	}

	offsetStr, hasOffsetKey := args["offset"]
	var offset uint64
	if hasOffsetKey && offsetStr != "" {
		val, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return CommonArgs{}, fmt.Errorf("invalid offset value: %s", offsetStr)
		}
		offset = val
	}

	batch := DefaultMaxPollRecords
	if b, err := strconv.Atoi(args["batch"]); err == nil && b > 0 {
		batch = b
	}

	wait := 0 * time.Millisecond
	if w, err := strconv.Atoi(args["wait_ms"]); err == nil && w > 0 {
		wait = time.Duration(w) * time.Millisecond
	}

	return CommonArgs{
		TopicName:       args["topic"],
		PartitionID:     pID,
		GroupName:       ch.resolveConsumerGroup(args["group"]),
		MemberID:        args["member"],
		Generation:      gen,
		HasOffset:       hasOffsetKey && offsetStr != "",
		Offset:          offset,
		BatchSize:       batch,
		WaitTimeout:     wait,
		AutoOffsetReset: strings.ToLower(args["autoOffsetReset"]),
	}, nil
}

func (ch *CommandHandler) validateConsumeArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing topic parameter")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing partition parameter")
	}
	if args["offset"] == "" {
		return fmt.Errorf("missing offset parameter")
	}
	if args["member"] == "" {
		return fmt.Errorf("missing member parameter")
	}
	return nil
}

func (ch *CommandHandler) validateStreamArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing topic parameter")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing partition parameter")
	}
	return nil
}
