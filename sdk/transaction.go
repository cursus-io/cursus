package sdk

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const transactionCommandTimeout = 10 * time.Second

type ProducerSession struct {
	TransactionalID string
	ProducerID      string
	Epoch           int64
}

type TransactionStatusInfo struct {
	TransactionalID string
	State           string
	Messages        int
	Offsets         int
}

// TransactionalProducer preserves the broker-owned producer session and
// serializes lifecycle calls for one transactional ID.
type TransactionalProducer struct {
	mu                    sync.Mutex
	client                *ConsumerClient
	session               ProducerSession
	needsReinitialization bool
}

func (c *ConsumerClient) NewTransactionalProducer(transactionalID string) (*TransactionalProducer, error) {
	session, err := c.InitProducerID(transactionalID)
	if err != nil {
		return nil, err
	}
	return &TransactionalProducer{client: c, session: session}, nil
}

func (p *TransactionalProducer) Session() ProducerSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

// Reinitialize obtains a new epoch and fences callers that still use the old session.
func (p *TransactionalProducer) Reinitialize() (ProducerSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, err := p.client.InitProducerID(p.session.TransactionalID)
	if err != nil {
		return ProducerSession{}, err
	}
	p.session = session
	p.needsReinitialization = false
	return session, nil
}

func (p *TransactionalProducer) Begin() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.needsReinitialization {
		session, err := p.client.InitProducerID(p.session.TransactionalID)
		if err != nil {
			return err
		}
		p.session = session
		p.needsReinitialization = false
	}
	return p.client.BeginTransaction(p.session.TransactionalID, p.session.ProducerID, p.session.Epoch)
}

func (p *TransactionalProducer) Publish(topic string, partition int, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	msg.ProducerID = p.session.ProducerID
	msg.Epoch = p.session.Epoch
	return p.client.TransactionalPublish(p.session.TransactionalID, topic, partition, msg)
}

func (p *TransactionalProducer) SendOffsets(topic, group, member string, generation int, offsets map[int]uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client.SendOffsetsToTransaction(p.session.TransactionalID, p.session.ProducerID, topic, group, member, generation, p.session.Epoch, offsets)
}

func (p *TransactionalProducer) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.client.EndTransaction(p.session.TransactionalID, p.session.ProducerID, p.session.Epoch, true); err != nil {
		return err
	}
	p.needsReinitialization = true
	return nil
}

func (p *TransactionalProducer) Abort() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.client.EndTransaction(p.session.TransactionalID, p.session.ProducerID, p.session.Epoch, false); err != nil {
		return err
	}
	p.needsReinitialization = true
	return nil
}

func (p *TransactionalProducer) Describe() (TransactionStatusInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client.DescribeTransaction(p.session.TransactionalID)
}

func (c *ConsumerClient) InitProducerID(transactionalID string) (ProducerSession, error) {
	if err := validateTransactionToken("transactional ID", transactionalID); err != nil {
		return ProducerSession{}, err
	}
	resp, err := c.execTxnCommand(transactionalID, fmt.Sprintf("INIT_PRODUCER_ID transactional_id=%s", transactionalID))
	if err != nil {
		return ProducerSession{}, err
	}
	return parseProducerSession(resp)
}

func parseProducerSession(resp string) (ProducerSession, error) {
	fields, err := parseOKResponse(resp)
	if err != nil {
		return ProducerSession{}, err
	}
	txnID := fields["transactional_id"]
	producerID := fields["producerId"]
	if producerID == "" {
		producerID = fields["producer_id"]
	}
	if txnID == "" || producerID == "" {
		return ProducerSession{}, fmt.Errorf("producer session response missing transactional_id or producerId: %s", resp)
	}
	epoch, err := strconv.ParseInt(fields["epoch"], 10, 64)
	if err != nil {
		return ProducerSession{}, fmt.Errorf("invalid producer session epoch %q: %w", fields["epoch"], err)
	}
	return ProducerSession{TransactionalID: txnID, ProducerID: producerID, Epoch: epoch}, nil
}

func (c *ConsumerClient) BeginTransaction(transactionalID, producerID string, epoch int64) error {
	if err := validateTransactionOwner(transactionalID, producerID); err != nil {
		return err
	}
	cmd := fmt.Sprintf("BEGIN_TXN transactional_id=%s producerId=%s epoch=%d", transactionalID, producerID, epoch)
	_, err := c.execTxnCommand(transactionalID, cmd)
	return err
}

func (c *ConsumerClient) TransactionalPublish(transactionalID, topic string, partition int, msg Message) error {
	if err := validateTransactionOwner(transactionalID, msg.ProducerID); err != nil {
		return err
	}
	if err := validateTransactionToken("topic", topic); err != nil {
		return err
	}
	if partition < -1 {
		return fmt.Errorf("transactional publish partition must be -1 or non-negative")
	}
	if msg.SeqNum == 0 {
		return fmt.Errorf("transactional publish sequence must be greater than zero")
	}
	if msg.Payload == "" {
		return fmt.Errorf("transactional publish payload is required")
	}
	if strings.ContainsAny(msg.Payload, "\r\n") {
		return fmt.Errorf("transactional publish payload must not contain line breaks")
	}

	cmd := fmt.Sprintf("TXN_PUBLISH transactional_id=%s topic=%s partition=%d producerId=%s seqNum=%d epoch=%d", transactionalID, topic, partition, msg.ProducerID, msg.SeqNum, msg.Epoch)
	if msg.Key != "" {
		if strings.ContainsAny(msg.Key, " \t\r\n") {
			return fmt.Errorf("transactional publish key must not contain whitespace")
		}
		cmd += fmt.Sprintf(" key=%s", msg.Key)
	}
	cmd += " message=" + msg.Payload
	_, err := c.execTxnCommand(transactionalID, cmd)
	return err
}

func (c *ConsumerClient) SendOffsetsToTransaction(transactionalID, producerID, topic, group, member string, generation int, epoch int64, offsets map[int]uint64) error {
	cmd, err := buildSendOffsetsToTransactionCommand(transactionalID, producerID, topic, group, member, generation, epoch, offsets)
	if err != nil {
		return err
	}
	_, err = c.execTxnCommand(transactionalID, cmd)
	return err
}

func buildSendOffsetsToTransactionCommand(transactionalID, producerID, topic, group, member string, generation int, epoch int64, offsets map[int]uint64) (string, error) {
	if err := validateTransactionOwner(transactionalID, producerID); err != nil {
		return "", err
	}
	if err := validateTransactionToken("topic", topic); err != nil {
		return "", err
	}
	if err := validateTransactionToken("group", group); err != nil {
		return "", err
	}
	if err := validateTransactionToken("member", member); err != nil {
		return "", err
	}
	if generation < 0 {
		return "", fmt.Errorf("transaction generation must be non-negative")
	}
	if len(offsets) == 0 {
		return "", fmt.Errorf("transaction offsets must not be empty")
	}

	partitions := make([]int, 0, len(offsets))
	for partition := range offsets {
		if partition < 0 {
			return "", fmt.Errorf("transaction offset partition must be non-negative: %d", partition)
		}
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	pairs := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		pairs = append(pairs, fmt.Sprintf("P%d:%d", partition, offsets[partition]))
	}
	return fmt.Sprintf("SEND_OFFSETS_TO_TXN transactional_id=%s producerId=%s epoch=%d topic=%s group=%s member=%s generation=%d %s", transactionalID, producerID, epoch, topic, group, member, generation, strings.Join(pairs, ",")), nil
}

func (c *ConsumerClient) EndTransaction(transactionalID, producerID string, epoch int64, commit bool) error {
	if err := validateTransactionOwner(transactionalID, producerID); err != nil {
		return err
	}
	result := "abort"
	if commit {
		result = "commit"
	}
	cmd := fmt.Sprintf("END_TXN transactional_id=%s producerId=%s epoch=%d result=%s", transactionalID, producerID, epoch, result)
	_, err := c.execTxnCommand(transactionalID, cmd)
	return err
}

// TransactionStatus preserves the original raw response API for compatibility.
func (c *ConsumerClient) TransactionStatus(transactionalID string) (string, error) {
	if err := validateTransactionToken("transactional ID", transactionalID); err != nil {
		return "", err
	}
	return c.execTxnCommand(transactionalID, fmt.Sprintf("TXN_STATUS transactional_id=%s", transactionalID))
}

// DescribeTransaction returns the broker transaction status as typed fields.
func (c *ConsumerClient) DescribeTransaction(transactionalID string) (TransactionStatusInfo, error) {
	resp, err := c.TransactionStatus(transactionalID)
	if err != nil {
		return TransactionStatusInfo{}, err
	}
	return parseTransactionStatus(resp)
}

func parseTransactionStatus(resp string) (TransactionStatusInfo, error) {
	fields, err := parseOKResponse(resp)
	if err != nil {
		return TransactionStatusInfo{}, err
	}
	status := TransactionStatusInfo{
		TransactionalID: fields["transactional_id"],
		State:           fields["state"],
	}
	if status.TransactionalID == "" || status.State == "" {
		return TransactionStatusInfo{}, fmt.Errorf("transaction status response missing transactional_id or state: %s", resp)
	}
	status.Messages, err = strconv.Atoi(fields["messages"])
	if err != nil || status.Messages < 0 {
		return TransactionStatusInfo{}, fmt.Errorf("invalid transaction message count %q", fields["messages"])
	}
	status.Offsets, err = strconv.Atoi(fields["offsets"])
	if err != nil || status.Offsets < 0 {
		return TransactionStatusInfo{}, fmt.Errorf("invalid transaction offset count %q", fields["offsets"])
	}
	return status, nil
}

func (c *ConsumerClient) execTxnCommand(transactionalID, cmd string) (string, error) {
	const maxRedirects = 5
	var lastErr error
	for attempt := 0; attempt < maxRedirects; attempt++ {
		conn, _, err := c.connectTransactionCoordinator(transactionalID)
		if err != nil {
			return "", err
		}
		resp, commandErr := executeTransactionCommand(conn, cmd)
		_ = conn.Close()
		if commandErr == nil {
			return resp, nil
		}

		var brokerErr *BrokerError
		if !errors.As(commandErr, &brokerErr) || !strings.EqualFold(brokerErr.Code, "NOT_COORDINATOR") {
			return "", commandErr
		}
		addr, addrErr := c.transactionCoordinatorAddress(brokerErr)
		if addrErr != nil {
			return "", fmt.Errorf("transaction coordinator redirect: %w", addrErr)
		}
		c.transactionCoordinators.Store(transactionalID, addr)
		lastErr = brokerErr
		if attempt+1 < maxRedirects {
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
	}
	return "", fmt.Errorf("transaction coordinator redirect limit exceeded: %w", lastErr)
}

func (c *ConsumerClient) connectTransactionCoordinator(transactionalID string) (net.Conn, string, error) {
	if cached, ok := c.transactionCoordinators.Load(transactionalID); ok {
		addr, _ := cached.(string)
		if addr != "" {
			conn, err := c.ConnectToAddr(addr)
			if err == nil {
				return conn, addr, nil
			}
			c.transactionCoordinators.Delete(transactionalID)
		}
	}
	return c.ConnectWithFailover()
}

func (c *ConsumerClient) transactionCoordinatorAddress(brokerErr *BrokerError) (string, error) {
	if brokerErr == nil {
		return "", fmt.Errorf("missing broker error")
	}
	host := brokerErr.Fields["host"]
	port := brokerErr.Fields["port"]
	parsedPort, err := strconv.Atoi(port)
	if host == "" || err != nil || parsedPort < 1 || parsedPort > 65535 {
		return "", fmt.Errorf("invalid NOT_COORDINATOR address host=%q port=%q", host, port)
	}
	if isLoopbackCoordinatorHost(host) && len(c.config.BrokerAddrs) > 0 {
		if bootstrapHost, _, splitErr := net.SplitHostPort(c.config.BrokerAddrs[0]); splitErr == nil && !isLoopbackCoordinatorHost(bootstrapHost) {
			host = bootstrapHost
		}
	}
	return net.JoinHostPort(host, port), nil
}

func executeTransactionCommand(conn net.Conn, cmd string) (string, error) {
	if conn == nil {
		return "", fmt.Errorf("transaction command connection is nil")
	}
	if err := conn.SetDeadline(time.Now().Add(transactionCommandTimeout)); err != nil {
		return "", fmt.Errorf("set transaction command deadline: %w", err)
	}
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return "", fmt.Errorf("send transaction command: %w", err)
	}
	resp, err := ReadWithLength(conn)
	if err != nil {
		return "", fmt.Errorf("read transaction response: %w", err)
	}
	respStr := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(respStr); ok {
		return "", brokerErr
	}
	if _, err := parseOKResponse(respStr); err != nil {
		return "", fmt.Errorf("unexpected transaction response: %s", respStr)
	}
	return respStr, nil
}

func validateTransactionOwner(transactionalID, producerID string) error {
	if err := validateTransactionToken("transactional ID", transactionalID); err != nil {
		return err
	}
	return validateTransactionToken("producer ID", producerID)
}

func validateTransactionToken(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if strings.ContainsAny(value, " \t\r\n") {
		return fmt.Errorf("%s must not contain whitespace", name)
	}
	return nil
}
