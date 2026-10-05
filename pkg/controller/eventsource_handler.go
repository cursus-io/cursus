package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func (ch *CommandHandler) handleAppendStream(cmd string) string {
	if ch.TxnManager == nil {
		return ch.handleAppendStreamLocked(cmd)
	}
	var response string
	ch.TxnManager.WithStreamKeyLock(eventStreamTopic(cmd, "APPEND_STREAM "), eventStreamKey(cmd, "APPEND_STREAM "), func() {
		response = ch.handleAppendStreamLocked(cmd)
	})
	return response
}

func (ch *CommandHandler) handleAppendStreamLocked(cmd string) string {
	if ch.TxnManager != nil {
		topicName := eventStreamTopic(cmd, "APPEND_STREAM ")
		key := eventStreamKey(cmd, "APPEND_STREAM ")
		if transactionalID, reserved := ch.TxnManager.StreamReservation(topicName, key); reserved {
			return fmt.Sprintf("ERROR: stream_version_reserved topic=%s key=%s transactional_id=%s", topicName, key, transactionalID)
		}
	}
	partition, errResp := ch.eventStreamPartition(cmd, "APPEND_STREAM ")
	if errResp != "" {
		return errResp
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		topicName := eventStreamTopic(cmd, "APPEND_STREAM ")
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
		t := ch.waitForTopic(topicName)
		if t == nil {
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
		}
		p, err := t.GetPartition(partition)
		if err != nil {
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
		}
		effectiveMinISR := t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas)
		releaseWrite, releaseMutation, replicationSnapshot, err := ch.preparePartitionLeaderSnapshot(topicName, partition, p, effectiveMinISR)
		if err != nil {
			return ch.partitionPreparationErrorResponse(err)
		}
		defer releaseWrite()
		defer releaseMutation()
		if indexResp := ch.reconcileEventSourceIndex(topicName, partition); indexResp != "" {
			return indexResp
		}
		result, errResp := ch.ESHandler.AppendStream(cmd, eventsource.AppendOptions{
			LeaderAppend: true,
			AfterCommit: func(topic string, partition int, hwm uint64) error {
				return ch.commitPartitionHWMAtEpoch(topic, partition, hwm, replicationSnapshot.Leader, replicationSnapshot.LeaderEpoch, replicationSnapshot.LifecycleEpoch)
			},
			AfterAppend: func(topic string, partition int, msg types.Message) error {
				msgCmd := types.MessageCommand{
					Topic:          topic,
					Partition:      partition,
					LifecycleEpoch: t.LifecycleEpoch,
					Messages:       []types.Message{msg},
					Acks:           "all",
					SequenceScope:  "partition",
				}
				return ch.Cluster.ReplicateToFollowers(topic, partition, msgCmd, effectiveMinISR)
			},
		})
		if errResp != "" {
			return errResp
		}
		return result.Response()
	}
	return ch.ESHandler.HandleAppendStream(cmd)
}

func (ch *CommandHandler) handleSaveSnapshot(cmd string) string {
	partition, errResp := ch.eventStreamPartition(cmd, "SAVE_SNAPSHOT ")
	if errResp != "" {
		return errResp
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		topicName := eventStreamTopic(cmd, "SAVE_SNAPSHOT ")
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
		t := ch.waitForTopic(topicName)
		if t == nil {
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
		}
		effectiveMinISR := t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas)
		if indexResp := ch.reconcileEventSourceIndex(topicName, partition); indexResp != "" {
			return indexResp
		}
		result, errResp := ch.ESHandler.SaveSnapshot(cmd, func(result eventsource.SnapshotResult) error {
			result.LifecycleEpoch = t.LifecycleEpoch
			payload, err := json.Marshal(result)
			if err != nil {
				return err
			}
			replicateCmd := fmt.Sprintf("REPLICATE_SNAPSHOT %spayload=%s", ch.internalAuthPrefix(), string(payload))
			return ch.Cluster.ReplicateCommandToFollowers(result.Topic, result.Partition, replicateCmd, effectiveMinISR)
		})
		if errResp != "" {
			return errResp
		}
		return result.Response()
	}
	return ch.ESHandler.HandleSaveSnapshot(cmd)
}

func (ch *CommandHandler) handleListSnapshots(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "LIST_SNAPSHOTS "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=LIST_SNAPSHOTS"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	if rawLimit := args["limit"]; rawLimit != "" {
		limit, parseErr := strconv.Atoi(rawLimit)
		if parseErr != nil || limit < 1 || limit > 1024 {
			return "ERROR: invalid_snapshot_page_limit"
		}
		afterKey := ""
		if cursor := args["after_key"]; cursor != "" {
			decoded, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
			if decodeErr != nil {
				return "ERROR: invalid_snapshot_page_cursor"
			}
			afterKey = string(decoded)
		}
		var expectedRevision uint64
		if rawRevision := args["revision"]; rawRevision != "" {
			expectedRevision, err = strconv.ParseUint(rawRevision, 10, 64)
			if err != nil || expectedRevision == 0 {
				return "ERROR: invalid_snapshot_catalog_revision"
			}
		}
		page, errResp := ch.ESHandler.ListSnapshotsPageAtRevision(topicName, partition, afterKey, limit, expectedRevision)
		if errResp != "" {
			return errResp
		}
		if t := ch.TopicManager.GetTopic(topicName); t != nil {
			for i := range page.Snapshots {
				page.Snapshots[i].LifecycleEpoch = t.LifecycleEpoch
			}
		}
		payload, marshalErr := json.Marshal(page)
		if marshalErr != nil {
			return fmt.Sprintf("ERROR: marshal_snapshot_page_failed reason=%q", marshalErr.Error())
		}
		return fmt.Sprintf("OK snapshot_page=%s", payload)
	}
	snaps, errResp := ch.ESHandler.ListSnapshots(topicName, partition)
	if errResp != "" {
		return errResp
	}
	if t := ch.TopicManager.GetTopic(topicName); t != nil {
		for i := range snaps {
			snaps[i].LifecycleEpoch = t.LifecycleEpoch
		}
	}
	payload, err := json.Marshal(snaps)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshots_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshots=%s", string(payload))
}

func (ch *CommandHandler) handleFetchSnapshot(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "FETCH_SNAPSHOT "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=FETCH_SNAPSHOT"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key command=FETCH_SNAPSHOT"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	snap, errResp := ch.ESHandler.FetchSnapshot(topicName, partition, key)
	if errResp != "" {
		return errResp
	}
	if snap == nil {
		return "OK snapshot=null"
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshot_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshot=%s", string(payload))
}

func (ch *CommandHandler) handleCatchupSnapshots(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "CATCHUP_SNAPSHOTS "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=CATCHUP_SNAPSHOTS"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	leaderAddr := args["leader"]
	if leaderAddr == "" {
		leaderAddr = ch.resolvePartitionLeaderAddr(topicName, partition)
	}
	if leaderAddr == "" {
		return "ERROR: leader_not_found"
	}
	if ch.Cluster == nil {
		return "ERROR: cluster_not_available"
	}
	applied := 0
	cursor := ""
	var revision uint64
	for pageNumber := 0; pageNumber < 1_000_000; pageNumber++ {
		request := fmt.Sprintf("LIST_SNAPSHOTS %stopic=%s partition=%d limit=256", ch.internalAuthPrefix(), topicName, partition)
		if cursor != "" {
			request += " after_key=" + cursor
		}
		if revision != 0 {
			request += fmt.Sprintf(" revision=%d", revision)
		}
		resp, forwardErr := ch.Cluster.ForwardCommandToBroker(leaderAddr, request)
		if forwardErr != nil {
			return fmt.Sprintf("ERROR: snapshot_catchup_failed reason=%q", forwardErr.Error())
		}
		if protocol.IsErrorResponse(resp) {
			return resp
		}
		payload := strings.TrimPrefix(resp, "OK snapshot_page=")
		if payload == resp {
			return fmt.Sprintf("ERROR: invalid_snapshot_catchup_response response=%q", resp)
		}
		var page eventsource.SnapshotPage
		if err := json.Unmarshal([]byte(payload), &page); err != nil {
			return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
		}
		if page.Revision == 0 || (revision != 0 && page.Revision != revision) {
			return "ERROR: snapshot_catalog_revision_changed"
		}
		revision = page.Revision
		for _, snap := range page.Snapshots {
			if snap.Topic == "" {
				snap.Topic = topicName
			}
			if snap.Partition == 0 {
				snap.Partition = partition
			}
			if errResp := ch.validateEventSnapshotLifecycle(snap); errResp != "" {
				return errResp
			}
			if errResp := ch.ESHandler.SaveSnapshotReplica(snap); errResp != "" {
				return errResp
			}
			applied++
		}
		if page.Done {
			return fmt.Sprintf("OK snapshots=%d", applied)
		}
		if len(page.Snapshots) == 0 {
			return "ERROR: snapshot_catchup_page_did_not_advance"
		}
		cursor = base64.RawURLEncoding.EncodeToString([]byte(page.Snapshots[len(page.Snapshots)-1].Key))
	}
	return "ERROR: snapshot_catchup_page_limit_exceeded"
}

func (ch *CommandHandler) handleReplicateSnapshot(cmd string) string {
	idx := strings.Index(cmd, "payload=")
	if idx == -1 {
		return "ERROR: missing_payload command=REPLICATE_SNAPSHOT"
	}
	payload := cmd[idx+8:]
	var snap eventsource.SnapshotResult
	if err := json.Unmarshal([]byte(payload), &snap); err != nil {
		return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
	}
	if snap.Topic == "" || snap.Key == "" {
		return "ERROR: invalid_snapshot_payload"
	}
	if errResp := ch.validateEventSnapshotLifecycle(snap); errResp != "" {
		return errResp
	}
	if errResp := ch.ESHandler.SaveSnapshotReplica(snap); errResp != "" {
		return errResp
	}
	return "OK"
}

func (ch *CommandHandler) validateEventSnapshotLifecycle(snap eventsource.SnapshotResult) string {
	if !ch.isDistributed() || ch.Cluster == nil || ch.Cluster.RaftManager == nil {
		return ""
	}
	fsmRef := ch.Cluster.RaftManager.GetFSM()
	if fsmRef == nil {
		return "ERROR: cluster_metadata_unavailable command=REPLICATE_SNAPSHOT"
	}
	definition, found := fsmRef.GetTopicDefinition(snap.Topic)
	if !found {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", snap.Topic)
	}
	if snap.LifecycleEpoch == 0 {
		if definition.LifecycleEpoch > topic.InitialLifecycleEpoch {
			return "ERROR: missing_topic_lifecycle_epoch command=REPLICATE_SNAPSHOT"
		}
		return ""
	}
	if snap.LifecycleEpoch != definition.LifecycleEpoch {
		return fmt.Sprintf("ERROR: STALE_TOPIC_LIFECYCLE_EPOCH current=%d requested=%d", definition.LifecycleEpoch, snap.LifecycleEpoch)
	}
	return ""
}

func (ch *CommandHandler) handleEventSourceRoutedCommand(cmd, prefix string, local func(string) string) string {
	partition, errResp := ch.eventStreamPartition(cmd, prefix)
	if errResp != "" {
		return errResp
	}
	topicName := eventStreamTopic(cmd, prefix)
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
		if indexResp := ch.reconcileEventSourceIndex(topicName, partition); indexResp != "" {
			return indexResp
		}
	}
	return local(cmd)
}

func (ch *CommandHandler) HandleReadStreamCommand(conn net.Conn, cmd string) {
	partition, errResp := ch.eventStreamPartition(cmd, "READ_STREAM ")
	if errResp != "" {
		writeReadStreamError(conn, errResp)
		return
	}
	topicName := eventStreamTopic(cmd, "READ_STREAM ")
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		if !ch.Cluster.IsAuthorized(topicName, partition) {
			leaderAddr := ch.resolvePartitionLeaderAddr(topicName, partition)
			writeReadStreamError(conn, fmt.Sprintf("ERROR: NOT_LEADER leader=%s", leaderAddr))
			return
		}
	}
	if indexResp := ch.reconcileEventSourceIndex(topicName, partition); indexResp != "" {
		writeReadStreamError(conn, indexResp)
		return
	}
	ch.ESHandler.HandleReadStream(cmd, conn)
}

// HandleReadStreamHistoryCommand preserves the leader and committed-index
// boundary used by READ_STREAM, but delegates to the snapshot-free history
// path.  It never substitutes READ_STREAM because that command may apply a
// snapshot and omit older source events.
func (ch *CommandHandler) HandleReadStreamHistoryCommand(conn net.Conn, cmd string) {
	partition, errResp := ch.eventStreamPartition(cmd, "READ_STREAM_HISTORY ")
	if errResp != "" {
		writeReadStreamError(conn, errResp)
		return
	}
	topicName := eventStreamTopic(cmd, "READ_STREAM_HISTORY ")
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		if !ch.Cluster.IsAuthorized(topicName, partition) {
			writeReadStreamError(conn, fmt.Sprintf("ERROR: NOT_LEADER leader=%s", ch.resolvePartitionLeaderAddr(topicName, partition)))
			return
		}
	}
	if indexResp := ch.reconcileEventSourceIndex(topicName, partition); indexResp != "" {
		writeReadStreamError(conn, indexResp)
		return
	}
	ch.ESHandler.HandleReadStreamHistory(cmd, conn)
}

func (ch *CommandHandler) reconcileEventSourceIndex(topicName string, partition int) string {
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	p, err := t.GetPartition(partition)
	if err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	if err := ch.ESHandler.PrepareCommittedIndex(topicName, partition); err != nil {
		return fmt.Sprintf("ERROR: stream_index_failed reason=%q", err.Error())
	}
	if err := ch.ESHandler.IndexCommittedToHWM(topicName, partition, p.GetHWM()); err != nil {
		return fmt.Sprintf("ERROR: stream_index_failed reason=%q", err.Error())
	}
	return ""
}
func (ch *CommandHandler) eventStreamPartition(cmd, prefix string) (int, string) {
	topicName := eventStreamTopic(cmd, prefix)
	if topicName == "" {
		return 0, "ERROR: missing_topic"
	}
	key := eventStreamKey(cmd, prefix)
	if key == "" {
		return 0, "ERROR: missing_key"
	}
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return 0, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return 0, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}
	partition := t.GetPartitionForMessage(types.Message{Key: key})
	if partition < 0 {
		return 0, "ERROR: no_partitions_available"
	}
	return partition, ""
}

func eventStreamTopic(cmd, prefix string) string {
	return parseKeyValueArgs(strings.TrimPrefix(cmd, prefix))["topic"]
}

func eventStreamKey(cmd, prefix string) string {
	return parseKeyValueArgs(strings.TrimPrefix(cmd, prefix))["key"]
}

func writeReadStreamError(conn net.Conn, msg string) {
	msg = strings.TrimSpace(msg)
	if !strings.HasPrefix(msg, "ERROR:") {
		msg = "ERROR: " + msg
	}
	_ = util.WriteWithLength(conn, []byte(msg))
}
