package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"slices"
	"strings"
)

type consumerCatalogGroupStatus struct {
	Status       string   `json:"status"`
	GroupName    string   `json:"group_name"`
	TopicName    string   `json:"topic_name"`
	Topics       []string `json:"topics"`
	TopicPattern string   `json:"topic_pattern"`
}

func (c *Consumer) validateExistingCatalog() error {
	if err := c.fetchMetadata(); err != nil {
		return fmt.Errorf("validate existing topic %q: %w", c.config.Topic, err)
	}
	status, err := c.fetchGroupStatus()
	if err != nil {
		return fmt.Errorf("validate existing consumer group %q: %w", c.config.GroupID, err)
	}
	if status.GroupName != c.config.GroupID || !catalogStatusContainsTopic(status, c.config.Topic) {
		return fmt.Errorf("%w: group=%q topic=%q", ErrConsumerCatalogMismatch, c.config.GroupID, c.config.Topic)
	}
	return nil
}

func (c *Consumer) fetchGroupStatus() (consumerCatalogGroupStatus, error) {
	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := c.getCoordinatorConnContext(requestCtx)
		if err != nil {
			lastErr = err
			break
		}
		status, retry, err := c.fetchGroupStatusOnConnection(requestCtx, conn)
		c.forgetCoordinatorConnection(conn)
		_ = conn.Close()
		if err == nil {
			return status, nil
		}
		lastErr = err
		if !retry || requestCtx.Err() != nil {
			break
		}
	}
	if errors.Is(lastErr, ErrConsumerGroupNotFound) {
		return consumerCatalogGroupStatus{}, ErrConsumerGroupNotFound
	}
	return consumerCatalogGroupStatus{}, lastErr
}

func (c *Consumer) fetchGroupStatusOnConnection(ctx context.Context, conn net.Conn) (consumerCatalogGroupStatus, bool, error) {
	cleanup, err := bindConnectionToContext(ctx, conn)
	if err != nil {
		return consumerCatalogGroupStatus{}, false, err
	}
	defer cleanup()

	command := fmt.Sprintf("GROUP_STATUS group=%s", c.config.GroupID)
	if err := WriteWithLength(conn, []byte(command)); err != nil {
		c.recordCoordinatorCommand(conn, "GROUP_STATUS", coordinatorResultFailure)
		return consumerCatalogGroupStatus{}, false, fmt.Errorf("send group status: %w", err)
	}
	response, err := ReadWithLength(conn)
	if err != nil {
		c.recordCoordinatorCommand(conn, "GROUP_STATUS", coordinatorResultFailure)
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			if errors.Is(brokerErr, ErrConsumerGroupNotFound) {
				return consumerCatalogGroupStatus{}, false, ErrConsumerGroupNotFound
			}
			if c.handleNotCoordinatorResponse(conn, "GROUP_STATUS", brokerErr) {
				return consumerCatalogGroupStatus{}, true, brokerErr
			}
		}
		return consumerCatalogGroupStatus{}, false, fmt.Errorf("read group status: %w", err)
	}

	var status consumerCatalogGroupStatus
	if err := json.Unmarshal(response, &status); err != nil {
		c.recordCoordinatorCommand(conn, "GROUP_STATUS", coordinatorResultFailure)
		return consumerCatalogGroupStatus{}, false, fmt.Errorf("decode group status: %w", err)
	}
	if !strings.EqualFold(status.Status, "OK") {
		c.recordCoordinatorCommand(conn, "GROUP_STATUS", coordinatorResultFailure)
		return consumerCatalogGroupStatus{}, false, fmt.Errorf("group status is not OK")
	}
	c.recordCoordinatorCommand(conn, "GROUP_STATUS", coordinatorResultSuccess)
	return status, false, nil
}

func catalogStatusContainsTopic(status consumerCatalogGroupStatus, topic string) bool {
	if status.TopicName == topic || slices.Contains(status.Topics, topic) {
		return true
	}
	if status.TopicPattern == "" {
		return false
	}
	escaped := regexp.QuoteMeta(status.TopicPattern)
	escaped = strings.ReplaceAll(escaped, `\*`, ".*")
	escaped = strings.ReplaceAll(escaped, `\?`, ".")
	matched, err := regexp.MatchString("^"+escaped+"$", topic)
	return err == nil && matched
}
