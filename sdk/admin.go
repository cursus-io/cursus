package sdk

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// AdminConfig configures bounded topic administration requests.
type AdminConfig struct {
	BrokerAddrs      []string `yaml:"broker_addrs" json:"broker_addrs"`
	MaxRetries       int      `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS   int      `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	RequestTimeoutMS int      `yaml:"request_timeout_ms" json:"request_timeout_ms"`

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	Principal string `yaml:"principal" json:"principal"`
	AuthToken string `yaml:"auth_token" json:"auth_token"`

	HandshakeTimeoutMS int    `yaml:"handshake_timeout_ms" json:"handshake_timeout_ms"`
	CompressionType    string `yaml:"compression_type" json:"compression_type"`
}

// NewDefaultAdminConfig returns conservative defaults for a local broker.
func NewDefaultAdminConfig() *AdminConfig {
	return &AdminConfig{
		BrokerAddrs:        []string{"localhost:9000"},
		MaxRetries:         3,
		RetryBackoffMS:     100,
		RequestTimeoutMS:   5000,
		HandshakeTimeoutMS: 5000,
		CompressionType:    "none",
	}
}

// TopicDefinitionPatch distinguishes omitted values from explicit zero, false,
// and empty ACL values. It is used for both CREATE and repeated-CREATE updates.
type TopicDefinitionPatch struct {
	Partitions        *int
	ReplicationFactor *int
	Idempotent        *bool
	EventSourcing     *bool
	CleanupPolicy     *TopicCleanupPolicy
	RetentionHours    *int
	RetentionBytes    *int64
	Partitioner       *string
	AuthPolicy        *string
	ReadACL           *[]string
	WriteACL          *[]string
}

// TopicDefinition is the authoritative definition returned by CREATE.
type TopicDefinition struct {
	Topic             string
	Revision          uint64
	LifecycleEpoch    uint64
	Partitions        int
	ReplicationFactor int
	Idempotent        bool
	EventSourcing     bool
	CleanupPolicy     TopicCleanupPolicy
	RetentionHours    int
	RetentionBytes    int64
	Partitioner       string
	AuthPolicy        string
	ReadACL           []string
	WriteACL          []string
}

// DeleteTopicOptions controls the explicit idempotency contract for deletion.
// IfExists=false reports topic_not_found when the topic does not exist.
type DeleteTopicOptions struct {
	IfExists bool
}

// DeleteTopicResult reports whether this request removed an existing topic.
// CleanupPending means logical deletion committed but broker-local storage
// cleanup still requires reconciliation or operator remediation.
type DeleteTopicResult struct {
	Topic          string
	Deleted        bool
	CleanupPending bool
}

// TruncateTopicOptions requires the caller's last observed definition
// revision so a delayed or duplicated request cannot erase a newer lifecycle.
type TruncateTopicOptions struct {
	ExpectedRevision uint64
}

// TruncateTopicResult reports the newly committed empty topic lifecycle.
// CleanupPending means the logical transition is committed while one or more
// broker-local cleanup operations remain fenced for reconciliation.
type TruncateTopicResult struct {
	Topic          string
	Truncated      bool
	Revision       uint64
	LifecycleEpoch uint64
	LEO            uint64
	HWM            uint64
	CleanupPending bool
}

// AdminClient performs bounded, broker-aware administrative requests.
type AdminClient struct {
	config    AdminConfig
	tlsConfig *tls.Config
}

type terminalAdminError struct {
	err error
}

func (e *terminalAdminError) Error() string { return e.err.Error() }
func (e *terminalAdminError) Unwrap() error { return e.err }

type ambiguousAdminError struct {
	err error
}

func (e *ambiguousAdminError) Error() string { return e.err.Error() }
func (e *ambiguousAdminError) Unwrap() error { return e.err }

func NewAdminClient(config *AdminConfig) (*AdminClient, error) {
	if config == nil {
		return nil, fmt.Errorf("admin config is nil")
	}
	if len(config.BrokerAddrs) == 0 {
		return nil, fmt.Errorf("no broker addresses available")
	}
	if err := validateWireClientSettings(config.CompressionType, config.Principal, config.AuthToken); err != nil {
		return nil, err
	}
	if err := validateTLSFiles(config.UseTLS, config.TLSCertPath, config.TLSKeyPath); err != nil {
		return nil, err
	}
	if config.MaxRetries < 0 {
		return nil, fmt.Errorf("max retries must be non-negative")
	}
	for _, addr := range config.BrokerAddrs {
		if strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("broker address must not be empty")
		}
	}

	client := &AdminClient{config: *config}
	client.config.BrokerAddrs = append([]string(nil), config.BrokerAddrs...)
	if client.config.RequestTimeoutMS <= 0 {
		client.config.RequestTimeoutMS = 5000
	}
	if client.config.HandshakeTimeoutMS < 0 {
		return nil, fmt.Errorf("handshake timeout must not be negative")
	}
	if client.config.RetryBackoffMS < 0 {
		return nil, fmt.Errorf("retry backoff must be non-negative")
	}
	if config.UseTLS {
		cert, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		client.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}
	return client, nil
}

func (c *AdminClient) CreateTopic(topic string, definition TopicDefinitionPatch) (TopicDefinition, error) {
	return c.CreateTopicContext(context.Background(), topic, definition)
}

func (c *AdminClient) CreateTopicContext(ctx context.Context, topic string, definition TopicDefinitionPatch) (TopicDefinition, error) {
	return c.applyTopicPatch(ctx, topic, definition)
}

func (c *AdminClient) UpdateTopic(topic string, patch TopicDefinitionPatch) (TopicDefinition, error) {
	return c.UpdateTopicContext(context.Background(), topic, patch)
}

func (c *AdminClient) UpdateTopicContext(ctx context.Context, topic string, patch TopicDefinitionPatch) (TopicDefinition, error) {
	return c.applyTopicPatch(ctx, topic, patch)
}

func (c *AdminClient) DeleteTopic(topic string, options DeleteTopicOptions) (DeleteTopicResult, error) {
	return c.DeleteTopicContext(context.Background(), topic, options)
}

func (c *AdminClient) DeleteTopicContext(ctx context.Context, topic string, options DeleteTopicOptions) (DeleteTopicResult, error) {
	if ctx == nil {
		return DeleteTopicResult{}, fmt.Errorf("admin context is nil")
	}
	command, err := buildAdminDeleteTopicCommand(topic, options)
	if err != nil {
		return DeleteTopicResult{}, err
	}
	response, err := c.execute(ctx, command, options.IfExists)
	if err != nil {
		return DeleteTopicResult{}, err
	}
	return parseDeleteTopicResponse(response)
}

func (c *AdminClient) TruncateTopic(topic string, options TruncateTopicOptions) (TruncateTopicResult, error) {
	return c.TruncateTopicContext(context.Background(), topic, options)
}

func (c *AdminClient) TruncateTopicContext(ctx context.Context, topic string, options TruncateTopicOptions) (TruncateTopicResult, error) {
	if ctx == nil {
		return TruncateTopicResult{}, fmt.Errorf("admin context is nil")
	}
	command, err := buildAdminTruncateTopicCommand(topic, options)
	if err != nil {
		return TruncateTopicResult{}, err
	}
	// Retry connection/routing failures, but never retry after the command may
	// have been written: expected_revision makes a replay safe, yet a committed
	// first attempt would correctly return a conflict rather than the result.
	response, err := c.execute(ctx, command, false)
	if err != nil {
		return TruncateTopicResult{}, err
	}
	return parseTruncateTopicResponse(response)
}

func (c *AdminClient) applyTopicPatch(ctx context.Context, topic string, patch TopicDefinitionPatch) (TopicDefinition, error) {
	if ctx == nil {
		return TopicDefinition{}, fmt.Errorf("admin context is nil")
	}
	command, err := buildAdminCreateTopicCommand(topic, patch)
	if err != nil {
		return TopicDefinition{}, err
	}
	response, err := c.execute(ctx, command, true)
	if err != nil {
		return TopicDefinition{}, err
	}
	return parseTopicDefinitionResponse(response)
}

func (c *AdminClient) execute(ctx context.Context, command string, retryAmbiguous bool) (string, error) {
	var lastErr error
	attempts := c.config.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		addr := c.config.BrokerAddrs[attempt%len(c.config.BrokerAddrs)]
		response, err := c.executeOnce(ctx, addr, command)
		if err == nil {
			return response, nil
		}
		lastErr = err
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) && !brokerErr.Retryable {
			return "", err
		}
		var terminalErr *terminalAdminError
		if errors.As(err, &terminalErr) {
			return "", terminalErr.err
		}
		var ambiguousErr *ambiguousAdminError
		if errors.As(err, &ambiguousErr) && !retryAmbiguous {
			return "", fmt.Errorf("admin request outcome is unknown and was not retried: %w", ambiguousErr.err)
		}
		if attempt+1 == attempts {
			break
		}
		backoff := time.Duration(c.config.RetryBackoffMS) * time.Millisecond
		if backoff <= 0 {
			continue
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", fmt.Errorf("admin request failed after %d attempt(s): %w", attempts, lastErr)
}

func (c *AdminClient) executeOnce(ctx context.Context, addr, command string) (string, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	deadline := time.Now().Add(time.Duration(c.config.RequestTimeoutMS) * time.Millisecond)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return "", fmt.Errorf("set admin request deadline: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stopCancellation()

	conn, err = openWireConnection(conn, c.config.HandshakeTimeoutMS, c.config.CompressionType)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			return "", brokerErr
		}
		if isRetryableAdminTransportError(err) {
			return "", fmt.Errorf("Wire v2 handshake with %s: %w", addr, err)
		}
		return "", &terminalAdminError{err: fmt.Errorf("Wire v2 handshake with %s: %w", addr, err)}
	}
	if err := authenticateConfiguredClient(conn, c.config.Principal, c.config.AuthToken); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			return "", brokerErr
		}
		if isRetryableAdminTransportError(err) {
			return "", fmt.Errorf("authentication with %s: %w", addr, err)
		}
		return "", &terminalAdminError{err: fmt.Errorf("authentication with %s: %w", addr, err)}
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return "", fmt.Errorf("restore admin request deadline: %w", err)
	}
	if err := WriteWithLength(conn, []byte(command)); err != nil {
		return "", &ambiguousAdminError{err: fmt.Errorf("send admin command to %s: %w", addr, err)}
	}
	response, err := ReadWithLength(conn)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", &ambiguousAdminError{err: fmt.Errorf("read admin response from %s: %w", addr, err)}
	}
	value := strings.TrimSpace(string(response))
	if brokerErr, ok := ParseBrokerError(value); ok {
		return "", brokerErr
	}
	if !hasOKStatus(value) {
		return "", &terminalAdminError{err: fmt.Errorf("unexpected admin response: %s", value)}
	}
	return value, nil
}

func isRetryableAdminTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func (c *AdminClient) dial(ctx context.Context, addr string) (net.Conn, error) {
	timeout := time.Duration(c.config.RequestTimeoutMS) * time.Millisecond
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	if !c.config.UseTLS {
		return conn, nil
	}
	if c.tlsConfig == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("TLS enabled but certificate not loaded")
	}
	tlsConn := tls.Client(conn, c.tlsConfig.Clone())
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("TLS handshake with %s: %w", addr, err)
	}
	return tlsConn, nil
}

func buildAdminCreateTopicCommand(topic string, patch TopicDefinitionPatch) (string, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return "", err
	}
	fields := []string{"CREATE", "topic=" + topic}
	if patch.Partitions != nil {
		if *patch.Partitions <= 0 {
			return "", fmt.Errorf("partitions must be positive")
		}
		fields = append(fields, "partitions="+strconv.Itoa(*patch.Partitions))
	}
	if patch.ReplicationFactor != nil {
		if *patch.ReplicationFactor <= 0 {
			return "", fmt.Errorf("replication factor must be positive")
		}
		fields = append(fields, "replication_factor="+strconv.Itoa(*patch.ReplicationFactor))
	}
	if patch.Idempotent != nil {
		fields = append(fields, "idempotent="+strconv.FormatBool(*patch.Idempotent))
	}
	if patch.EventSourcing != nil {
		fields = append(fields, "event_sourcing="+strconv.FormatBool(*patch.EventSourcing))
	}
	if patch.CleanupPolicy != nil {
		normalized, err := normalizeSDKCleanupPolicy(*patch.CleanupPolicy)
		if err != nil {
			return "", err
		}
		if normalized == "" {
			return "", fmt.Errorf("cleanup policy must not be empty when supplied")
		}
		fields = append(fields, "cleanup_policy="+normalized)
	}
	if patch.RetentionHours != nil {
		if *patch.RetentionHours < 0 {
			return "", fmt.Errorf("retention hours must be non-negative")
		}
		fields = append(fields, "retention_hours="+strconv.Itoa(*patch.RetentionHours))
	}
	if patch.RetentionBytes != nil {
		if *patch.RetentionBytes < 0 {
			return "", fmt.Errorf("retention bytes must be non-negative")
		}
		fields = append(fields, "retention_bytes="+strconv.FormatInt(*patch.RetentionBytes, 10))
	}
	if patch.Partitioner != nil {
		switch *patch.Partitioner {
		case "hash_key", "round_robin":
			fields = append(fields, "partitioner="+*patch.Partitioner)
		default:
			return "", fmt.Errorf("invalid partitioner %q", *patch.Partitioner)
		}
	}
	if patch.AuthPolicy != nil {
		switch *patch.AuthPolicy {
		case "open", "deny_write", "deny_read", "acl":
			fields = append(fields, "auth_policy="+*patch.AuthPolicy)
		default:
			return "", fmt.Errorf("invalid auth policy %q", *patch.AuthPolicy)
		}
	}
	var err error
	fields, err = appendAdminACLField(fields, "read_acl", patch.ReadACL)
	if err != nil {
		return "", err
	}
	fields, err = appendAdminACLField(fields, "write_acl", patch.WriteACL)
	if err != nil {
		return "", err
	}
	return strings.Join(fields, " "), nil
}

func buildAdminDeleteTopicCommand(topic string, options DeleteTopicOptions) (string, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return "", err
	}
	command := "DELETE topic=" + topic
	if options.IfExists {
		command += " if_exists=true"
	}
	return command, nil
}

func buildAdminTruncateTopicCommand(topic string, options TruncateTopicOptions) (string, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return "", err
	}
	if options.ExpectedRevision == 0 {
		return "", fmt.Errorf("expected revision must be greater than zero")
	}
	return "TRUNCATE topic=" + topic + " expected_revision=" + strconv.FormatUint(options.ExpectedRevision, 10), nil
}

func appendAdminACLField(fields []string, name string, values *[]string) ([]string, error) {
	if values == nil {
		return fields, nil
	}
	for _, value := range *values {
		if !isSafeTopicOptionValue(value, false) || strings.Contains(value, ",") {
			return nil, fmt.Errorf("invalid ACL principal %q", value)
		}
	}
	return append(fields, name+"="+strings.Join(*values, ",")), nil
}

func parseTopicDefinitionResponse(response string) (TopicDefinition, error) {
	fields, err := parseOKResponse(response)
	if err != nil {
		return TopicDefinition{}, err
	}
	definition := TopicDefinition{Topic: fields["topic"]}
	if definition.Topic == "" {
		return TopicDefinition{}, fmt.Errorf("missing topic")
	}
	if definition.Partitions, err = parseAdminInt(fields, "partitions"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.ReplicationFactor, err = parseAdminInt(fields, "replication_factor"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.RetentionHours, err = parseAdminInt(fields, "retention_hours"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.RetentionBytes, err = parseAdminInt64(fields, "retention_bytes"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.Revision, err = parseAdminUint64(fields, "revision"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.LifecycleEpoch, err = parseAdminUint64(fields, "lifecycle_epoch"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.Idempotent, err = parseAdminBool(fields, "idempotent"); err != nil {
		return TopicDefinition{}, err
	}
	if definition.EventSourcing, err = parseAdminBool(fields, "event_sourcing"); err != nil {
		return TopicDefinition{}, err
	}
	definition.CleanupPolicy = TopicCleanupPolicy(fields["cleanup_policy"])
	definition.Partitioner = fields["partitioner"]
	definition.AuthPolicy = fields["auth_policy"]
	if definition.CleanupPolicy == "" || definition.Partitioner == "" || definition.AuthPolicy == "" {
		return TopicDefinition{}, fmt.Errorf("incomplete topic policy in response")
	}
	definition.ReadACL = splitAdminACL(fields["read_acl"])
	definition.WriteACL = splitAdminACL(fields["write_acl"])
	return definition, nil
}

func parseDeleteTopicResponse(response string) (DeleteTopicResult, error) {
	fields, err := parseOKResponse(response)
	if err != nil {
		return DeleteTopicResult{}, err
	}
	result := DeleteTopicResult{Topic: fields["topic"]}
	if result.Topic == "" {
		return DeleteTopicResult{}, fmt.Errorf("missing topic")
	}
	if result.Deleted, err = parseAdminBool(fields, "deleted"); err != nil {
		return DeleteTopicResult{}, err
	}
	if value, present := fields["cleanup_pending"]; present {
		result.CleanupPending, err = strconv.ParseBool(value)
		if err != nil {
			return DeleteTopicResult{}, fmt.Errorf("invalid cleanup_pending: %w", err)
		}
	}
	return result, nil
}

func parseTruncateTopicResponse(response string) (TruncateTopicResult, error) {
	fields, err := parseOKResponse(response)
	if err != nil {
		return TruncateTopicResult{}, err
	}
	result := TruncateTopicResult{Topic: fields["topic"]}
	if result.Topic == "" {
		return TruncateTopicResult{}, fmt.Errorf("missing topic")
	}
	if result.Truncated, err = parseAdminBool(fields, "truncated"); err != nil {
		return TruncateTopicResult{}, err
	}
	if result.Revision, err = parseAdminUint64(fields, "revision"); err != nil {
		return TruncateTopicResult{}, err
	}
	if result.LifecycleEpoch, err = parseAdminUint64(fields, "lifecycle_epoch"); err != nil {
		return TruncateTopicResult{}, err
	}
	if result.LEO, err = parseAdminUint64(fields, "leo"); err != nil {
		return TruncateTopicResult{}, err
	}
	if result.HWM, err = parseAdminUint64(fields, "hwm"); err != nil {
		return TruncateTopicResult{}, err
	}
	if value, present := fields["cleanup_pending"]; present {
		result.CleanupPending, err = strconv.ParseBool(value)
		if err != nil {
			return TruncateTopicResult{}, fmt.Errorf("invalid cleanup_pending: %w", err)
		}
	}
	return result, nil
}

func parseAdminInt(fields map[string]string, name string) (int, error) {
	value, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing %s", name)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func parseAdminInt64(fields map[string]string, name string) (int64, error) {
	value, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing %s", name)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func parseAdminUint64(fields map[string]string, name string) (uint64, error) {
	value, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing %s", name)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func parseAdminBool(fields map[string]string, name string) (bool, error) {
	value, ok := fields[name]
	if !ok {
		return false, fmt.Errorf("missing %s", name)
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func splitAdminACL(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
