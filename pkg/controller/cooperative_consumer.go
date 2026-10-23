package controller

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/topic"
)

func (ch *CommandHandler) cooperativeEnabled() bool {
	if ch.isDistributed() {
		return ch.Cluster.RaftManager != nil && ch.Cluster.RaftManager.GetFSM() != nil && ch.Cluster.RaftManager.GetFSM().CooperativeConsumerEnabled()
	}
	return ch.Config != nil && ch.Config.ConsumerCooperativeEnabled
}

func (ch *CommandHandler) cooperativeTopic(name string) (topic.Definition, error) {
	if ch.isDistributed() {
		if ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
			return topic.Definition{}, fmt.Errorf("ERROR: coordinator_not_available")
		}
		definition, ok := ch.Cluster.RaftManager.GetFSM().GetTopicDefinition(name)
		if !ok {
			return definition, fmt.Errorf("ERROR: topic_not_found")
		}
		return definition, nil
	}
	if ch.TopicManager != nil {
		if t := ch.TopicManager.GetTopic(name); t != nil {
			return t.Definition(), nil
		}
	}
	return topic.Definition{}, fmt.Errorf("ERROR: topic_not_found")
}

func (ch *CommandHandler) handleCooperativeMembership(args map[string]string, action string) string {
	if !ch.cooperativeEnabled() {
		return "ERROR: cooperative_not_enabled"
	}
	group, member := args["group"], args["member"]
	if group == "" || member == "" || strings.ContainsAny(member, " \r\n\t") {
		return "ERROR: invalid_cooperative_payload"
	}
	if ch.isDistributed() {
		addr, local, err := ch.checkCoordinator(group)
		if err != nil {
			return coordinatorUnavailableResponse
		}
		if !local {
			return notCoordinatorResponse(addr)
		}
	}
	if ch.Coordinator == nil {
		return coordinatorUnavailableResponse
	}
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic"
	}
	definition, err := ch.cooperativeTopic(topicName)
	if err != nil {
		return err.Error()
	}
	if ch.Coordinator.GetGroup(group) == nil {
		if action != "join" {
			return "ERROR: cooperative_fenced"
		}
		if err := ch.Coordinator.RegisterGroup(topicName, group, definition.Partitions); err != nil {
			return formatCoordinatorError(err)
		}
	}
	snapshot := ch.Coordinator.ExportGroupState(group)
	if snapshot == nil {
		return coordinatorUnavailableResponse
	}
	// This wire/API version supports one explicit topic per group. Reject a
	// mismatched subscription before modifying durable membership.
	if snapshot.TopicName != topicName || len(snapshot.Topics) != 0 || snapshot.TopicPattern != "" {
		return "ERROR: invalid_cooperative_payload"
	}
	names := append([]string(nil), snapshot.Topics...)
	if len(names) == 0 {
		names = []string{snapshot.TopicName}
	}
	sort.Strings(names)
	var partitions []coordinator.CooperativePartition
	for _, name := range names {
		def, err := ch.cooperativeTopic(name)
		if err != nil {
			return err.Error()
		}
		for p := 0; p < def.Partitions; p++ {
			partitions = append(partitions, coordinator.CooperativePartition{Topic: name, Partition: p, LifecycleEpoch: def.LifecycleEpoch})
		}
	}
	registration, session := uint64(0), uint64(0)
	if action != "join" {
		registration, err = strconv.ParseUint(args["registration_epoch"], 10, 64)
		if err != nil || registration == 0 {
			return "ERROR: invalid_cooperative_payload"
		}
		session, err = strconv.ParseUint(args["session"], 10, 64)
		if err != nil || session == 0 {
			return "ERROR: invalid_cooperative_payload"
		}
	}
	var acks []coordinator.CooperativeRevocation
	if args["revoked"] != "" {
		if err := protocol.DecodeCooperative(args["revoked"], &acks); err != nil {
			return err.Error()
		}
	}
	result, err := ch.Coordinator.ReconcileCooperative(group, member, registration, session, partitions, acks, action == "leave")
	if err != nil {
		return formatCoordinatorError(err)
	}
	return "OK cooperative=" + protocol.EncodeCooperative(result)
}

func parseCooperativeToken(args map[string]string) (*protocol.CooperativeToken, error) {
	if args["ownership"] == "" {
		return nil, nil
	}
	var token protocol.CooperativeToken
	if err := protocol.DecodeCooperative(args["ownership"], &token); err != nil {
		return nil, err
	}
	p, err := strconv.Atoi(args["partition"])
	if err != nil || token.RegistrationEpoch == 0 || token.Grant.Member != args["member"] || token.Grant.Partition.Topic != args["topic"] || token.Grant.Partition.Partition != p || token.Grant.Session == 0 || token.Grant.Epoch == 0 {
		return nil, fmt.Errorf("ERROR: cooperative_fenced")
	}
	return &token, nil
}

func (ch *CommandHandler) validateCooperativeToken(group string, token *protocol.CooperativeToken) error {
	if ch.Coordinator == nil {
		return fmt.Errorf("ERROR: coordinator_not_available")
	}
	if token == nil {
		return ch.Coordinator.ValidateCooperativeRead(group, 0, nil)
	}
	definition, err := ch.cooperativeTopic(token.Grant.Partition.Topic)
	if err != nil {
		return err
	}
	if definition.LifecycleEpoch != token.Grant.Partition.LifecycleEpoch || token.Grant.Partition.Partition >= definition.Partitions {
		return fmt.Errorf("ERROR: cooperative_fenced")
	}
	return ch.Coordinator.ValidateCooperativeRead(group, token.RegistrationEpoch, &token.Grant)
}

// Used for both local and remote data leaders; a missing token must also be
// checked at the authoritative coordinator, never a stale follower group map.
func (ch *CommandHandler) validateCooperativeFetch(args CommonArgs) error {
	if args.Ownership == nil && ch.isDistributed() && (ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil || !ch.Cluster.RaftManager.GetFSM().CooperativeConsumerEnabled()) {
		return nil
	}
	if ch.isDistributed() {
		if ch.Cluster.Router == nil {
			return fmt.Errorf("ERROR: coordinator_not_available")
		}
		cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s member=%s cooperative_check=true", args.TopicName, args.PartitionID, args.GroupName, args.MemberID)
		if args.Ownership != nil {
			cmd += " ownership=" + protocol.EncodeCooperative(args.Ownership)
		}
		response, err := ch.Cluster.Router.ForwardToCoordinator(args.GroupName, cmd)
		if err != nil {
			return fmt.Errorf("ERROR: coordinator_not_available reason=%q", err.Error())
		}
		if response != "OK validated=true" {
			return fmt.Errorf("%s", response)
		}
		return nil
	}
	if ch.Coordinator == nil && args.Ownership == nil {
		return nil
	}
	return ch.validateCooperativeToken(args.GroupName, args.Ownership)
}

func (ch *CommandHandler) handleCooperativeReadCheck(args map[string]string) string {
	if ch.isDistributed() {
		addr, local, err := ch.checkCoordinator(args["group"])
		if err != nil {
			return coordinatorUnavailableResponse
		}
		if !local {
			return notCoordinatorResponse(addr)
		}
	}
	token, err := parseCooperativeToken(args)
	if err != nil {
		return err.Error()
	}
	if err := ch.validateCooperativeToken(args["group"], token); err != nil {
		return err.Error()
	}
	return "OK validated=true"
}

func (ch *CommandHandler) handleCooperativeCommit(args map[string]string, partition int, offset uint64) string {
	token, err := parseCooperativeToken(args)
	if err != nil {
		return err.Error()
	}
	if token == nil {
		return "ERROR: cooperative_protocol_required"
	}
	if err := ch.validateCooperativeToken(args["group"], token); err != nil {
		return err.Error()
	}
	if strings.EqualFold(args["validate_only"], "true") {
		if !strings.EqualFold(args["ownership_only"], "true") {
			if current, ok := ch.Coordinator.GetOffset(args["group"], args["topic"], partition); ok && offset < current {
				return "ERROR: offset_regression"
			}
		}
		return "OK validated=true"
	}
	if err := ch.Coordinator.CommitCooperative(args["group"], token.RegistrationEpoch, token.Grant, offset); err != nil {
		return formatCoordinatorError(err)
	}
	return "OK"
}
