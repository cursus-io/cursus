package sdk

import (
	"fmt"
	"sort"
	"strings"
)

func (c *ConsumerClient) BeginTransaction(transactionalID, producerID string, epoch int64) error {
	cmd := fmt.Sprintf("BEGIN_TXN transactional_id=%s producerId=%s epoch=%d", transactionalID, producerID, epoch)
	_, err := c.execTxnCommand(cmd)
	return err
}

func (c *ConsumerClient) TransactionalPublish(transactionalID, topic string, partition int, msg Message) error {
	cmd := fmt.Sprintf("TXN_PUBLISH transactional_id=%s topic=%s partition=%d producerId=%s seqNum=%d epoch=%d", transactionalID, topic, partition, msg.ProducerID, msg.SeqNum, msg.Epoch)
	if msg.Key != "" {
		cmd += fmt.Sprintf(" key=%s", msg.Key)
	}
	cmd += " message=" + msg.Payload
	_, err := c.execTxnCommand(cmd)
	return err
}

func (c *ConsumerClient) SendOffsetsToTransaction(transactionalID, topic, group string, offsets map[int]uint64) error {
	pairs := make([]string, 0, len(offsets))
	partitions := make([]int, 0, len(offsets))
	for partition := range offsets {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	for _, partition := range partitions {
		pairs = append(pairs, fmt.Sprintf("P%d:%d", partition, offsets[partition]))
	}
	cmd := fmt.Sprintf("SEND_OFFSETS_TO_TXN transactional_id=%s topic=%s group=%s %s", transactionalID, topic, group, strings.Join(pairs, ","))
	_, err := c.execTxnCommand(cmd)
	return err
}

func (c *ConsumerClient) EndTransaction(transactionalID string, commit bool) error {
	result := "abort"
	if commit {
		result = "commit"
	}
	cmd := fmt.Sprintf("END_TXN transactional_id=%s result=%s", transactionalID, result)
	_, err := c.execTxnCommand(cmd)
	return err
}

func (c *ConsumerClient) TransactionStatus(transactionalID string) (string, error) {
	return c.execTxnCommand(fmt.Sprintf("TXN_STATUS transactional_id=%s", transactionalID))
}

func (c *ConsumerClient) execTxnCommand(cmd string) (string, error) {
	conn, _, err := c.ConnectWithFailover()
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return "", fmt.Errorf("send transaction command: %w", err)
	}
	resp, err := ReadWithLength(conn)
	if err != nil {
		return "", fmt.Errorf("read transaction response: %w", err)
	}
	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return "", fmt.Errorf("transaction command failed: %s", respStr)
	}
	if !strings.HasPrefix(respStr, "OK") {
		return "", fmt.Errorf("unexpected transaction response: %s", respStr)
	}
	return respStr, nil
}
