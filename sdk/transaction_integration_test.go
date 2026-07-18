package sdk

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTransactionalProducerAgainstBroker(t *testing.T) {
	rawAddrs := strings.TrimSpace(os.Getenv("CURSUS_INTEGRATION_ADDRS"))
	if rawAddrs == "" {
		t.Skip("set CURSUS_INTEGRATION_ADDRS to run broker integration validation")
	}
	addrs := strings.Split(rawAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "sdk-transaction-" + suffix
	transactionalID := "sdk-transactional-" + suffix
	offsetGroup := "sdk-offset-" + suffix
	visibilityGroup := "sdk-visible-" + suffix

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = addrs
	client, err := NewConsumerClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := integrationOKCommand(client, fmt.Sprintf("CREATE topic=%s partitions=1", topic)); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_, _ = integrationOKCommand(client, fmt.Sprintf("DELETE topic=%s", topic))
	})

	offsetGeneration, offsetMember := integrationJoinGroup(t, client, topic, offsetGroup)
	visibilityGeneration, visibilityMember := integrationJoinGroup(t, client, topic, visibilityGroup)

	producer, err := client.NewTransactionalProducer(transactionalID)
	if err != nil {
		t.Fatalf("initialize transactional producer: %v", err)
	}
	if err := producer.Begin(); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := producer.Publish(topic, 0, Message{SeqNum: 1, Payload: "sdk-transaction-record"}); err != nil {
		t.Fatalf("transactional publish: %v", err)
	}
	if err := producer.SendOffsets(topic, offsetGroup, offsetMember, offsetGeneration, map[int]uint64{0: 1}); err != nil {
		t.Fatalf("send offsets to transaction: %v", err)
	}

	open, err := producer.Describe()
	if err != nil {
		t.Fatalf("describe open transaction: %v", err)
	}
	if open.State != "open" || open.Messages != 1 || open.Offsets != 1 {
		t.Fatalf("unexpected open transaction: %+v", open)
	}
	if messages := integrationConsumePartition(t, client, addrs, topic, visibilityGroup, visibilityMember, visibilityGeneration); len(messages) != 0 {
		t.Fatalf("open transaction became visible: %+v", messages)
	}

	if err := producer.Commit(); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if err := producer.Commit(); err != nil {
		t.Fatalf("retry transaction commit: %v", err)
	}
	committed, err := producer.Describe()
	if err != nil {
		t.Fatalf("describe committed transaction: %v", err)
	}
	if committed.State != "committed" {
		t.Fatalf("unexpected committed transaction: %+v", committed)
	}

	messages := integrationConsumePartition(t, client, addrs, topic, visibilityGroup, visibilityMember, visibilityGeneration)
	if len(messages) != 1 || messages[0].Payload != "sdk-transaction-record" {
		t.Fatalf("unexpected committed records: %+v", messages)
	}
	offsetResp, err := integrationOKCommand(client, fmt.Sprintf("FETCH_OFFSET topic=%s partition=0 group=%s", topic, offsetGroup))
	if err != nil {
		t.Fatalf("fetch committed offset: %v", err)
	}
	offsetFields, err := parseOKResponse(offsetResp)
	if err != nil || offsetFields["offset"] != "1" {
		t.Fatalf("unexpected committed offset response %q: %v", offsetResp, err)
	}
}

func integrationJoinGroup(t *testing.T, client *ConsumerClient, topic, group string) (int, string) {
	t.Helper()
	joinResp, err := integrationOKCommand(client, fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=sdk-client", topic, group))
	if err != nil {
		t.Fatalf("join group %s: %v", group, err)
	}
	fields := parseOKFields(joinResp)
	generation, err := strconv.Atoi(fields["generation"])
	if err != nil || fields["member"] == "" {
		t.Fatalf("invalid join response %q", joinResp)
	}
	member := fields["member"]
	if _, err := integrationOKCommand(client, fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d", topic, group, member, generation)); err != nil {
		t.Fatalf("sync group %s: %v", group, err)
	}
	return generation, member
}

func integrationOKCommand(client *ConsumerClient, command string) (string, error) {
	addr := ""
	for attempt := 0; attempt < 8; attempt++ {
		var conn net.Conn
		var connErr error
		var connAddr string
		if addr == "" {
			conn, connAddr, connErr = client.ConnectWithFailover()
		} else {
			conn, connErr = client.ConnectToAddr(addr)
			connAddr = addr
		}
		if connErr != nil {
			return "", connErr
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if err := WriteWithLength(conn, EncodeMessage("", command)); err != nil {
			_ = conn.Close()
			return "", err
		}
		response, err := ReadWithLength(conn)
		_ = conn.Close()
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(string(response))
		if brokerErr, ok := ParseBrokerError(value); ok {
			if strings.EqualFold(brokerErr.Code, "NOT_COORDINATOR") {
				host, port := brokerErr.Fields["host"], brokerErr.Fields["port"]
				if host == "" || port == "" {
					return "", brokerErr
				}
				addr = netJoinCoordinator(client, host, port)
				continue
			}
			return "", brokerErr
		}
		if !hasOKStatus(value) {
			return "", fmt.Errorf("unexpected response from %s: %s", connAddr, value)
		}
		return value, nil
	}
	return "", fmt.Errorf("coordinator redirect limit exceeded for %s", command)
}

func netJoinCoordinator(client *ConsumerClient, host, port string) string {
	if isLoopbackCoordinatorHost(host) && len(client.config.BrokerAddrs) > 0 {
		bootstrapHost, _, err := net.SplitHostPort(client.config.BrokerAddrs[0])
		if err == nil && !isLoopbackCoordinatorHost(bootstrapHost) {
			host = bootstrapHost
		}
	}
	return net.JoinHostPort(host, port)
}

func integrationConsumePartition(t *testing.T, client *ConsumerClient, addrs []string, topic, group, member string, generation int) []Message {
	t.Helper()
	command := fmt.Sprintf("CONSUME topic=%s partition=0 offset=0 group=%s generation=%d member=%s autoOffsetReset=earliest batch=10", topic, group, generation, member)
	var lastErr error
	for _, addr := range addrs {
		conn, err := client.ConnectToAddr(addr)
		if err != nil {
			lastErr = err
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := WriteWithLength(conn, EncodeMessage("", command)); err != nil {
			_ = conn.Close()
			lastErr = err
			continue
		}
		response, err := ReadWithLength(conn)
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if brokerErr, ok := ParseBrokerError(strings.TrimSpace(string(response))); ok {
			lastErr = brokerErr
			if strings.EqualFold(brokerErr.Code, "NOT_LEADER") {
				continue
			}
			break
		}
		messages, _, _, err := DecodeBatchMessages(response)
		if err == nil {
			return messages
		}
		lastErr = err
	}
	t.Fatalf("consume partition through SDK: %v", lastErr)
	return nil
}
