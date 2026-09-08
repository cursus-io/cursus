package sdk

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// TopicPartition identifies one partition without losing its topic identity.
type TopicPartition struct {
	Topic     string
	Partition int
}

type GroupAssignment struct {
	Generation int
	MemberID   string
	Partitions []TopicPartition
}

// RegisterGroupSubscription registers either an explicit topic set or a topic
// pattern. Exactly one selector must be supplied.
func (c *ConsumerClient) RegisterGroupSubscription(group string, topics []string, pattern string) error {
	if err := validateTransactionToken("group", group); err != nil {
		return err
	}
	if (len(topics) == 0) == (pattern == "") {
		return fmt.Errorf("specify exactly one of topics or pattern")
	}
	cmd := fmt.Sprintf("REGISTER_GROUP group=%s", group)
	if len(topics) > 0 {
		canonical := append([]string(nil), topics...)
		sort.Strings(canonical)
		for i, topic := range canonical {
			if err := validateSDKTopicName(topic); err != nil {
				return err
			}
			if i > 0 && canonical[i-1] == topic {
				return fmt.Errorf("duplicate topic %q", topic)
			}
		}
		cmd += " topics=" + strings.Join(canonical, ",")
	} else {
		if strings.ContainsAny(pattern, " \t\r\n,=") {
			return fmt.Errorf("topic pattern contains unsupported characters")
		}
		cmd += " pattern=" + pattern
	}
	_, err := c.execTxnCommand(group, cmd)
	return err
}

func (c *ConsumerClient) JoinGroupSubscription(group, member string) (GroupAssignment, error) {
	if err := validateTransactionToken("group", group); err != nil {
		return GroupAssignment{}, err
	}
	if err := validateTransactionToken("member", member); err != nil {
		return GroupAssignment{}, err
	}
	resp, err := c.execTxnCommand(group, fmt.Sprintf("JOIN_GROUP group=%s member=%s", group, member))
	if err != nil {
		return GroupAssignment{}, err
	}
	fields := parseOKFields(resp)
	generation, err := strconv.Atoi(fields["generation"])
	if err != nil {
		return GroupAssignment{}, fmt.Errorf("invalid group generation %q", fields["generation"])
	}
	partitions, err := parseTopicAssignments(fields["topic_assignments"])
	if err != nil {
		return GroupAssignment{}, err
	}
	return GroupAssignment{Generation: generation, MemberID: fields["member"], Partitions: partitions}, nil
}

func parseTopicAssignments(value string) ([]TopicPartition, error) {
	if value == "" {
		return nil, nil
	}
	result := make([]TopicPartition, 0)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(item, ":P", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid topic assignment %q", item)
		}
		partition, err := strconv.Atoi(parts[1])
		if err != nil || partition < 0 {
			return nil, fmt.Errorf("invalid topic assignment %q", item)
		}
		result = append(result, TopicPartition{Topic: parts[0], Partition: partition})
	}
	return result, nil
}

// SendTopicOffsets stages offsets from multiple topics in one transaction.
func (p *TransactionalProducer) SendTopicOffsets(group, member string, generation int, offsets map[TopicPartition]uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	byTopic := make(map[string]map[int]uint64)
	for tp, offset := range offsets {
		if byTopic[tp.Topic] == nil {
			byTopic[tp.Topic] = make(map[int]uint64)
		}
		byTopic[tp.Topic][tp.Partition] = offset
	}
	topics := make([]string, 0, len(byTopic))
	for topic := range byTopic {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		if err := p.client.SendOffsetsToTransaction(p.session.TransactionalID, p.session.ProducerID, topic, group, member, generation, p.session.Epoch, byTopic[topic]); err != nil {
			return err
		}
	}
	return nil
}
