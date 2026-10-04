package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

var ErrStreamRejected = errors.New("stream rejected")

const (
	ReadIsolationCommitted   = "read_committed"
	ReadIsolationUncommitted = "read_uncommitted"
)

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	argsMap := parseKeyValueArgs(rawCmd[8:])
	if authResp := ch.authenticateInline(argsMap, ctx); authResp != "" {
		return 0, fmt.Errorf("%s", authResp)
	}
	if authResp := ch.authorizeClientPermissions("CONSUME", argsMap, ctx, PermissionTopicRead, PermissionGroup); authResp != "" {
		return 0, fmt.Errorf("%s", authResp)
	}
	if err := ch.validateConsumeArgs(argsMap); err != nil {
		return 0, err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return 0, err
	}
	if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
		if err.Error() == "not partition leader" {
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
	remainingBytes := wire.MaxFetchDecodedBytes
	fetchBudgetExhausted := false
	var allMessages []types.Message

	readAvailable := func() error {
		for _, tName := range matchedTopics {
			if totalStreamed >= cArgs.BatchSize {
				break
			}

			remainingBatch := cArgs.BatchSize - totalStreamed
			messages, decodedBytes, err := ch.readFromTopicBounded(tName, cArgs, ctx, remainingBatch, remainingBytes, totalStreamed == 0)
			if err != nil {
				return err
			}
			remainingBytes -= decodedBytes
			if remainingBytes <= 0 || (len(messages) == 0 && decodedBytes > 0) {
				fetchBudgetExhausted = true
			}
			if len(messages) > 0 {
				allMessages = append(allMessages, messages...)
				totalStreamed += len(messages)
			}
			if fetchBudgetExhausted {
				break
			}
		}
		return nil
	}

	if cArgs.WaitTimeout > 0 {
		requestCtx := ctx.RequestContext()
		waitTimeout, err := effectiveConsumeWait(requestCtx, cArgs.WaitTimeout)
		if err != nil {
			return 0, err
		}
		deadline := time.Now().Add(waitTimeout)
		for waitTimeout > 0 && totalStreamed == 0 && !fetchBudgetExhausted {
			// Subscribe before reading so an append between the read and wait
			// closes the captured generation instead of being missed.
			notifications, err := ch.consumeNotifications(matchedTopics, cArgs.PartitionID)
			if err != nil {
				return 0, err
			}
			if err := readAvailable(); err != nil {
				if ch.writeConsumeReadError(conn, err) {
					return 0, nil
				}
				return 0, err
			}
			if totalStreamed > 0 || fetchBudgetExhausted {
				break
			}
			if err := waitForConsumeNotification(requestCtx, time.Until(deadline), notifications); err != nil {
				if errors.Is(err, errConsumeWaitElapsed) {
					break
				}
				return 0, err
			}
		}
	} else if err := readAvailable(); err != nil {
		if ch.writeConsumeReadError(conn, err) {
			return totalStreamed, nil
		}
		return totalStreamed, err
	}

	batchData, err := util.EncodeBatchMessages(cArgs.TopicName, cArgs.PartitionID, "1", false, allMessages)
	if err != nil {
		return 0, fmt.Errorf("failed to encode batch: %w", err)
	}

	if err := util.WriteWithLength(conn, batchData); err != nil {
		return 0, fmt.Errorf("failed to stream batch: %w", err)
	}

	return totalStreamed, nil
}

func (ch *CommandHandler) writeConsumeReadError(conn net.Conn, err error) bool {
	var offsetErr *types.OffsetOutOfRangeError
	if !errors.As(err, &offsetErr) {
		return false
	}

	resp := fmt.Sprintf("ERROR: OFFSET_OUT_OF_RANGE requested=%d earliest=%d latest=%d", offsetErr.Requested, offsetErr.Earliest, offsetErr.Latest)
	if writeErr := util.WriteWithLength(conn, []byte(resp)); writeErr != nil {
		util.Error("failed to send offset out-of-range response: %v", writeErr)
	}
	return true
}
func (ch *CommandHandler) readFromTopic(topicName string, cArgs CommonArgs, ctx *ClientContext, batchSize int) ([]types.Message, error) {
	messages, _, err := ch.readFromTopicBounded(topicName, cArgs, ctx, batchSize, wire.MaxFetchDecodedBytes, true)
	return messages, err
}

func (ch *CommandHandler) readFromTopicBounded(topicName string, cArgs CommonArgs, ctx *ClientContext, batchSize, maxBytes int, allowOversizedFirst bool) ([]types.Message, int, error) {
	t, p, err := ch.getTopicAndPartition(topicName, cArgs.PartitionID)
	if err != nil {
		return nil, 0, err
	}
	if authResp := ch.authorizeTopicRead(t.PolicySnapshot(), ctx); authResp != "" {
		return nil, 0, fmt.Errorf("%s topic=%s", authResp, topicName)
	}

	// Check stability even on a reused connection: cached positions cannot
	// authorize reads while the group's coordinator is resolving a transaction.
	savedOffset, found, err := ch.stableGroupOffset(cArgs.GroupName, topicName, cArgs.PartitionID)
	if err != nil {
		return nil, 0, err
	}
	currentOffset := savedOffset
	cacheKey := consumerOffsetCacheKey(topicName, cArgs)
	if cached, ok := ctx.OffsetCache[cacheKey]; ok {
		if cached > currentOffset {
			currentOffset = cached
		}
	} else if !found {
		currentOffset = resetConsumerOffset(p, cArgs)
	}

	messages, decodedBytes, nextScan, err := readPartitionPage(p, currentOffset, batchSize, maxBytes, allowOversizedFirst, cArgs.ReadIsolation)
	if err != nil {
		util.Error("Failed to read messages from topic %s: %v", topicName, err)
		return nil, 0, err
	}

	// This connection-local cursor can advance through aborted/control records
	// without acknowledging or exposing them to the consumer group.
	if nextScan > currentOffset {
		ctx.OffsetCache[cacheKey] = nextScan
	}

	return messages, decodedBytes, nil
}

var errConsumeWaitElapsed = errors.New("consume wait elapsed")

func effectiveConsumeWait(ctx context.Context, requested time.Duration) (time.Duration, error) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, nil
		}
		if remaining < requested {
			return remaining, nil
		}
	}
	return requested, nil
}

func (ch *CommandHandler) consumeNotifications(topicNames []string, partitionID int) ([]<-chan struct{}, error) {
	notifications := make([]<-chan struct{}, 0, len(topicNames))
	for _, topicName := range topicNames {
		_, partition, err := ch.getTopicAndPartition(topicName, partitionID)
		if err != nil {
			return nil, err
		}
		_, notification := partition.MessageNotification()
		notifications = append(notifications, notification)
	}
	return notifications, nil
}

func waitForConsumeNotification(ctx context.Context, timeout time.Duration, notifications []<-chan struct{}) error {
	if timeout <= 0 {
		return errConsumeWaitElapsed
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	if len(notifications) > 65_534 {
		return fmt.Errorf("consume topic pattern matches too many partitions: %d", len(notifications))
	}
	cases := make([]reflect.SelectCase, 0, len(notifications)+2)
	cases = append(cases,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer.C)},
	)
	for _, notification := range notifications {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(notification)})
	}
	selected, _, _ := reflect.Select(cases)
	switch selected {
	case 0:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errConsumeWaitElapsed
		}
		return ctx.Err()
	case 1:
		return errConsumeWaitElapsed
	default:
		return nil
	}
}

func consumerOffsetCacheKey(topicName string, args CommonArgs) string {
	return fmt.Sprintf("%q/%q/%q/%d/%d", args.GroupName, args.MemberID, topicName, args.Generation, args.PartitionID)
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
	if authResp := ch.authenticateInline(argsMap, ctx); authResp != "" {
		return fmt.Errorf("%s", authResp)
	}
	if authResp := ch.authorizeClientPermissions("STREAM", argsMap, ctx, PermissionTopicRead, PermissionGroup); authResp != "" {
		return fmt.Errorf("%s", authResp)
	}
	if err := ch.validateStreamArgs(argsMap); err != nil {
		return err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return err
	}
	ctx.ConsumerGroup = cArgs.GroupName

	if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
		if err.Error() == "not partition leader" {
			return ErrStreamRejected
		}
		return err
	}

	ctx.Generation = cArgs.Generation
	ctx.MemberID = cArgs.MemberID

	t, p, err := ch.getTopicAndPartition(cArgs.TopicName, cArgs.PartitionID)
	if err != nil {
		return err
	}
	if authResp := ch.authorizeTopicRead(t.PolicySnapshot(), ctx); authResp != "" {
		return fmt.Errorf("%s topic=%s", authResp, cArgs.TopicName)
	}

	actualOffset, err := ch.resolveOffset(p, cArgs.TopicName, cArgs)
	if err != nil {
		return err
	}

	streamKey := fmt.Sprintf("%s:%d:%s", cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName)
	streamConn := stream.NewStreamConnection(conn, cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName, actualOffset)
	streamConn.SetBatchSize(cArgs.BatchSize)
	streamConn.SetInterval(100 * time.Millisecond)

	streamConn.SetMessageSource(p.MessageNotification)

	// The stream invokes readFn serially. Keep scan progress distinct from its
	// delivered offset so empty filtered pages cannot strand later visible data.
	scanOffset := actualOffset
	readFn := func(offset uint64, max int) ([]types.Message, error) {
		stable, found, err := ch.stableGroupOffset(cArgs.GroupName, cArgs.TopicName, cArgs.PartitionID)
		if err != nil {
			return nil, err
		}
		if found && stable > offset {
			offset = stable
		}
		if offset > scanOffset {
			scanOffset = offset
		}
		messages, _, next, err := readPartitionPage(p, scanOffset, max, wire.MaxFetchDecodedBytes, true, cArgs.ReadIsolation)
		if err == nil {
			scanOffset = next
		}
		return messages, err
	}

	return ch.StreamManager.AddStream(streamKey, streamConn, readFn)
}

func readPartitionPage(p *topic.Partition, offset uint64, maxRecords, maxBytes int, allowOversizedFirst bool, isolation string) ([]types.Message, int, uint64, error) {
	if isolation == ReadIsolationUncommitted {
		messages, bytes, err := p.ReadMessagesBounded(offset, maxRecords, maxBytes, allowOversizedFirst)
		next := offset
		if err == nil && len(messages) > 0 {
			next = messages[len(messages)-1].Offset + 1
		}
		return messages, bytes, next, err
	}
	return p.ReadCommittedPage(offset, maxRecords, maxBytes, allowOversizedFirst)
}

func (ch *CommandHandler) validateStreamSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[7:])
	if args["topic"] == "" || args["partition"] == "" || args["group"] == "" {
		return ch.fail(raw, "ERROR: invalid_stream_syntax")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return ch.fail(raw, "ERROR: "+err.Error())
	}
	return STREAM_DATA_SIGNAL
}

func (ch *CommandHandler) validateConsumeSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[8:])
	if args["topic"] == "" || args["partition"] == "" || args["offset"] == "" || args["member"] == "" {
		return ch.fail(raw, "ERROR: invalid_consume_syntax")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return ch.fail(raw, "ERROR: "+err.Error())
	}
	return STREAM_DATA_SIGNAL
}

// checkPartitionLeaderOrRedirect checks if this broker is the leader for the given partition.
// If not, writes a NOT_LEADER redirect with the partition leader's client address.
func (ch *CommandHandler) checkPartitionLeaderOrRedirect(conn net.Conn, topicName string, partitionID int) error {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.IsAuthorized(topicName, partitionID) {
		return nil
	}

	leaderAddr := ch.resolvePartitionLeaderAddr(topicName, partitionID)
	if leaderAddr == "" {
		errResp := fmt.Sprintf("ERROR: leader_not_found topic=%s partition=%d", topicName, partitionID)
		if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
			return fmt.Errorf("failed to send missing partition leader response: %w", err)
		}
		return fmt.Errorf("partition leader not found")
	}
	errResp := fmt.Sprintf("ERROR: NOT_LEADER leader=%s", leaderAddr)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send partition leader redirect: %w", err)
	}
	return fmt.Errorf("not partition leader")
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
		if ch.Config.AdvertisedClientHost != "" {
			host = ch.Config.AdvertisedClientHost
		}
		port := ch.Config.BrokerPort
		if ch.Config.AdvertisedBrokerPort > 0 {
			port = ch.Config.AdvertisedBrokerPort
		}
		serviceLeader = net.JoinHostPort(host, strconv.Itoa(port))
	}

	errResp := fmt.Sprintf("ERROR: NOT_LEADER leader=%s", serviceLeader)
	util.Warn("leader redirect: %s", errResp)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send leader redirect: %w", err)
	}
	return fmt.Errorf("not leader")
}

func (ch *CommandHandler) getTopicAndPartition(topicName string, partitionID int) (*topic.Topic, *topic.Partition, error) {
	if ch.TopicManager == nil {
		return nil, nil, fmt.Errorf("topic_manager_not_available")
	}

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

func (ch *CommandHandler) resolveConsumerGroup(groupName, topicName string) string {
	if groupName == "" || groupName == "-" {
		return implicitDefaultGroupName(topicName)
	}
	return groupName
}

func implicitDefaultGroupName(topicName string) string {
	return "default-group@" + topicName
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
	ReadIsolation   string
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
	if rawBatch, supplied := args["batch"]; supplied {
		b, err := strconv.Atoi(rawBatch)
		if err != nil || b <= 0 {
			return CommonArgs{}, fmt.Errorf("ERROR: invalid_batch maximum=%d", wire.MaxFetchRecords)
		}
		if b > wire.MaxFetchRecords {
			return CommonArgs{}, fmt.Errorf("ERROR: fetch_batch_too_large requested=%d maximum=%d", b, wire.MaxFetchRecords)
		}
		batch = b
	}

	wait := 0 * time.Millisecond
	if rawWait, supplied := args["wait_ms"]; supplied {
		w, err := strconv.Atoi(rawWait)
		if err != nil || w <= 0 {
			return CommonArgs{}, fmt.Errorf("ERROR: invalid_wait_ms maximum=%d", wire.MaxFetchWaitMillis)
		}
		if w > wire.MaxFetchWaitMillis {
			return CommonArgs{}, fmt.Errorf("ERROR: fetch_wait_too_large requested=%d maximum=%d", w, wire.MaxFetchWaitMillis)
		}
		wait = time.Duration(w) * time.Millisecond
	}

	return CommonArgs{
		TopicName:       args["topic"],
		PartitionID:     pID,
		GroupName:       ch.resolveConsumerGroup(args["group"], args["topic"]),
		MemberID:        args["member"],
		Generation:      gen,
		HasOffset:       hasOffsetKey && offsetStr != "",
		Offset:          offset,
		BatchSize:       batch,
		WaitTimeout:     wait,
		AutoOffsetReset: strings.ToLower(args["autoOffsetReset"]),
		ReadIsolation:   normalizeReadIsolation(args["isolation"]),
	}, nil
}

func normalizeReadIsolation(value string) string {
	value = strings.ToLower(value)
	if value == "" {
		return ReadIsolationCommitted
	}
	return value
}

func (ch *CommandHandler) validateConsumeArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing_topic")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing_partition")
	}
	if args["offset"] == "" {
		return fmt.Errorf("missing_offset")
	}
	if args["member"] == "" {
		return fmt.Errorf("missing_member")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return err
	}
	return nil
}

func (ch *CommandHandler) validateStreamArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing_topic")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing_partition")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return err
	}
	return nil
}

func validateReadIsolation(value string) error {
	switch normalizeReadIsolation(value) {
	case ReadIsolationCommitted, ReadIsolationUncommitted:
		return nil
	default:
		return fmt.Errorf("invalid_isolation isolation=%s", value)
	}
}
