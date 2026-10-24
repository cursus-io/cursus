package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/google/uuid"
)

// CooperativeMessage carries the ownership that delivered this record. Keep it
// with deferred work: Commit and SendCooperativeOffsets never substitute a newer
// partition grant. Handler cancellation cannot undo external side effects.
type CooperativeMessage struct {
	Message
	Group     string
	Ownership protocol.CooperativeToken
}

// Membership traffic has its own persistent connection so offset commits cannot
// queue ahead of heartbeats. The operation lane is bounded by request deadlines.
type cooperativeRPC struct {
	gate chan struct{}
	conn net.Conn
}

func newCooperativeRPC() cooperativeRPC { return cooperativeRPC{gate: make(chan struct{}, 1)} }
func (r *cooperativeRPC) close() {
	r.gate <- struct{}{}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	<-r.gate
}

type cooperativeWorker struct {
	token      protocol.CooperativeToken
	cancel     context.CancelFunc
	done       chan struct{}
	revocation *protocol.CooperativeRevocation
}

// CooperativeStats counts actual worker construction and payload work. Replay
// and processing gaps require application record identities/timestamps.
type CooperativeStats struct {
	WorkerStarts, WorkerStops, FetchedRecords, FetchedPayloadBytes, DeliveredRecords uint64
	AutoCommitRequests, AutoCommitPartitions, AutoCommitWaitNanos                    uint64
}

// CooperativeConsumer is explicitly opted in and requires a capable broker.
// Run uses per-partition workers in both polling and streaming modes. Revocation
// cancels a handler's context and waits for its return before acknowledging it;
// handlers must not leave untracked background work running after returning.
type CooperativeConsumer struct {
	membershipRPC    cooperativeRPC
	operationsRPC    cooperativeRPC
	transport        *Consumer
	mu               sync.Mutex
	membership       protocol.CooperativeMembership
	workers          map[int]*cooperativeWorker
	started          atomic.Bool
	closed           atomic.Bool
	starts           atomic.Uint64
	stops            atomic.Uint64
	fetched          atomic.Uint64
	bytes            atomic.Uint64
	delivered        atomic.Uint64
	failures         chan error
	commitMu         sync.Mutex
	commitPending    []*cooperativeCommitRequest
	commitRunning    bool
	commitRequests   atomic.Uint64
	commitPartitions atomic.Uint64
	commitWait       atomic.Uint64
}

func NewCooperativeConsumer(cfg *ConsumerConfig) (*CooperativeConsumer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("consumer config is nil")
	}
	copied := *cfg
	copied.BrokerAddrs = append([]string(nil), cfg.BrokerAddrs...)
	if err := validateSDKTopicName(copied.Topic); err != nil {
		return nil, err
	}
	if err := validateTransactionToken("group", copied.GroupID); err != nil {
		return nil, err
	}
	transport, err := NewConsumer(&copied)
	if err != nil {
		return nil, err
	}
	transport.coordinatorAddr = copied.CoordinatorAddr
	return &CooperativeConsumer{membershipRPC: newCooperativeRPC(), operationsRPC: newCooperativeRPC(), transport: transport, workers: make(map[int]*cooperativeWorker), failures: make(chan error, 1)}, nil
}

func (c *CooperativeConsumer) Stats() CooperativeStats {
	return CooperativeStats{c.starts.Load(), c.stops.Load(), c.fetched.Load(), c.bytes.Load(), c.delivered.Load(), c.commitRequests.Load(), c.commitPartitions.Load(), c.commitWait.Load()}
}

// Run is single-use. Cancel ctx to stop; it returns only after handlers finish
// and a bounded leave attempt completes. A blocked revoked handler does not
// block heartbeats or workers for retained partitions.
func (c *CooperativeConsumer) Run(ctx context.Context, handler func(context.Context, CooperativeMessage) error) error {
	if ctx == nil || handler == nil {
		return fmt.Errorf("context and handler are required")
	}
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("cooperative consumer already started")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.transport.rootCancel()
	stop := context.AfterFunc(ctx, c.transport.rootCancel)
	defer stop()
	member := uuid.NewString()
	c.membership.Member = member
	defer func() {
		c.mu.Lock()
		workers := make([]*cooperativeWorker, 0, len(c.workers))
		for _, w := range c.workers {
			w.cancel()
			workers = append(workers, w)
		}
		c.mu.Unlock()
		for _, w := range workers {
			<-w.done
		}
		c.mu.Lock()
		membership := c.membership
		c.workers = make(map[int]*cooperativeWorker)
		c.closed.Store(true)
		c.mu.Unlock()
		defer c.membershipRPC.close()
		defer c.operationsRPC.close()
		if membership.Session != 0 {
			leaveCtx, done := context.WithTimeout(context.Background(), c.transport.requestTimeout())
			defer done()
			_, _ = c.membershipRequest(leaveCtx, "LEAVE_GROUP", membership, nil)
		}
	}()
	interval := time.Duration(c.transport.config.HeartbeatIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	for {
		c.mu.Lock()
		membership := c.membership
		acks := c.completedRevocationsLocked()
		c.mu.Unlock()
		command := "HEARTBEAT"
		if membership.Session == 0 {
			command = "JOIN_GROUP"
		}
		next, err := c.membershipRequest(ctx, command, membership, acks)
		if err == nil {
			if err = c.reconcile(ctx, next, handler); err != nil {
				return err
			}
		} else if !cooperativeRetryable(err) {
			return err
		} else {
			LogDebug("cooperative %s retry: %v", command, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.failures:
			return err
		case <-time.After(interval):
		}
	}
}

func (c *CooperativeConsumer) completedRevocationsLocked() []protocol.CooperativeRevocation {
	var acks []protocol.CooperativeRevocation
	for _, w := range c.workers {
		if w.revocation != nil {
			select {
			case <-w.done:
				acks = append(acks, *w.revocation)
			default:
			}
		}
	}
	return acks
}

func sameCooperativeToken(a, b protocol.CooperativeToken) bool {
	return a.RegistrationEpoch == b.RegistrationEpoch && a.Grant.Partition == b.Grant.Partition && a.Grant.Member == b.Grant.Member && a.Grant.Session == b.Grant.Session && a.Grant.Epoch == b.Grant.Epoch
}

func (c *CooperativeConsumer) reconcile(ctx context.Context, next protocol.CooperativeMembership, handler func(context.Context, CooperativeMessage) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if next.Member != c.membership.Member || next.Session == 0 || next.RegistrationEpoch == 0 || (c.membership.Session != 0 && (next.Session != c.membership.Session || next.RegistrationEpoch != c.membership.RegistrationEpoch || next.Revision < c.membership.Revision)) {
		return fmt.Errorf("invalid cooperative membership response")
	}
	grants := make(map[int]protocol.CooperativeGrant, len(next.Grants))
	for _, g := range next.Grants {
		if g.Member != next.Member || g.Session != next.Session || g.Partition.Topic != c.transport.config.Topic || g.Epoch == 0 {
			return fmt.Errorf("invalid cooperative grant")
		}
		if _, ok := grants[g.Partition.Partition]; ok {
			return fmt.Errorf("duplicate cooperative grant")
		}
		grants[g.Partition.Partition] = g
	}
	c.membership = next
	for pid, w := range c.workers {
		g, ok := grants[pid]
		token := protocol.CooperativeToken{RegistrationEpoch: next.RegistrationEpoch, Grant: g}
		if ok && sameCooperativeToken(w.token, token) && g.RevokeRevision == 0 && w.revocation == nil {
			continue
		}
		w.cancel()
		if ok && sameCooperativeToken(w.token, token) && g.RevokeRevision != 0 {
			w.revocation = &protocol.CooperativeRevocation{Partition: g.Partition, GrantEpoch: g.Epoch, RevokeRevision: g.RevokeRevision}
			continue
		}
		select {
		case <-w.done:
			delete(c.workers, pid)
		default:
		}
	}
	for pid, g := range grants {
		if g.RevokeRevision != 0 || c.workers[pid] != nil {
			continue
		}
		workerCtx, cancel := context.WithCancel(ctx)
		w := &cooperativeWorker{token: protocol.CooperativeToken{RegistrationEpoch: next.RegistrationEpoch, Grant: g}, cancel: cancel, done: make(chan struct{})}
		c.workers[pid] = w
		c.starts.Add(1)
		go func() {
			defer close(w.done)
			defer c.stops.Add(1)
			if err := c.runPartition(workerCtx, w, handler); err != nil && workerCtx.Err() == nil {
				select {
				case c.failures <- err:
				default:
				}
			}
		}()
	}
	return nil
}

func cooperativeRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var broker *BrokerError
	if errors.As(err, &broker) {
		return broker.Retryable || broker.Code == "NOT_COORDINATOR" || broker.Code == "NOT_LEADER"
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "connect") || strings.Contains(err.Error(), "EOF")
}

func (c *CooperativeConsumer) command(ctx context.Context, command string, requireCapability bool) (string, error) {
	requestCtx, cancel := boundedRequestContext(ctx, c.transport.requestTimeout())
	defer cancel()
	rpc := &c.operationsRPC
	if strings.HasPrefix(command, "JOIN_GROUP ") || strings.HasPrefix(command, "HEARTBEAT ") || strings.HasPrefix(command, "LEAVE_GROUP ") {
		rpc = &c.membershipRPC
	}
	select {
	case rpc.gate <- struct{}{}:
	case <-requestCtx.Done():
		return "", requestCtx.Err()
	}
	defer func() { <-rpc.gate }()
	if err := requestCtx.Err(); err != nil {
		return "", err
	}
	if rpc == &c.operationsRPC && c.closed.Load() {
		return "", context.Canceled
	}
	if rpc.conn == nil {
		conn, err := c.transport.getCoordinatorConnContext(requestCtx)
		if err != nil {
			return "", err
		}
		rpc.conn = conn
	}
	conn := rpc.conn
	reusable := false
	defer func() {
		if !reusable {
			_ = conn.Close()
			rpc.conn = nil
		}
	}()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return "", err
	}
	defer cleanup()
	if requireCapability {
		if _, err := NegotiateProtocol(conn, ProtocolNegotiation{Features: []string{"consumer_cooperative_v1"}, RequireFeatures: true}); err != nil {
			return "", err
		}
	}
	if err = WriteWithLength(conn, []byte(command)); err != nil {
		return "", err
	}
	data, err := ReadWithLength(conn)
	if err != nil {
		var broker *BrokerError
		if errors.As(err, &broker) {
			c.transport.handleNotCoordinatorError(broker)
		}
		return "", err
	}
	response := strings.TrimSpace(string(data))
	if broker, ok := ParseBrokerError(response); ok {
		c.transport.handleNotCoordinatorError(broker)
		return "", broker
	}
	if !hasOKStatus(response) {
		return "", fmt.Errorf("invalid cooperative response: %s", response)
	}
	reusable = true
	return response, nil
}

func (c *CooperativeConsumer) membershipRequest(ctx context.Context, command string, m protocol.CooperativeMembership, acks []protocol.CooperativeRevocation) (protocol.CooperativeMembership, error) {
	request := fmt.Sprintf("%s protocol=cooperative topic=%s group=%s member=%s", command, c.transport.config.Topic, c.transport.config.GroupID, m.Member)
	if command != "JOIN_GROUP" {
		request += fmt.Sprintf(" registration_epoch=%d session=%d", m.RegistrationEpoch, m.Session)
	}
	if len(acks) > 0 {
		request += " revoked=" + protocol.EncodeCooperative(acks)
	}
	response, err := c.command(ctx, request, command == "JOIN_GROUP")
	if err != nil {
		return protocol.CooperativeMembership{}, err
	}
	fields, err := parseOKResponse(response)
	if err != nil {
		return protocol.CooperativeMembership{}, err
	}
	var next protocol.CooperativeMembership
	err = protocol.DecodeCooperative(fields["cooperative"], &next)
	return next, err
}

// Commit persists a next offset using the exact grant that delivered message.
func (c *CooperativeConsumer) Commit(ctx context.Context, message CooperativeMessage, nextOffset uint64) error {
	if message.Group != c.transport.config.GroupID || message.Ownership.Grant.Partition.Topic != c.transport.config.Topic || nextOffset <= message.Offset {
		return fmt.Errorf("invalid cooperative commit")
	}
	pid := message.Ownership.Grant.Partition.Partition
	c.mu.Lock()
	w := c.workers[pid]
	valid := w != nil && sameCooperativeToken(w.token, message.Ownership)
	c.mu.Unlock()
	if !valid {
		return fmt.Errorf("cooperative ownership superseded")
	}
	cmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s member=%s offset=%d ownership=%s", c.transport.config.Topic, pid, message.Group, message.Ownership.Grant.Member, nextOffset, protocol.EncodeCooperative(message.Ownership))
	_, err := c.command(ctx, cmd, false)
	return err
}

func (c *CooperativeConsumer) fetchPartition(ctx context.Context, w *cooperativeWorker, batches chan<- []Message) error {
	cfg := c.transport.config
	pid := w.token.Grant.Partition.Partition
	var conn net.Conn
	var release func()
	closeConn := func() {
		if release != nil {
			release()
			release = nil
		}
		if conn != nil {
			_ = conn.Close()
			conn = nil
		}
	}
	defer closeConn()
	var offset uint64
	initialized := false
	retry := newBackoff(max(cfg.PollInterval, 100*time.Millisecond), time.Second)
	for ctx.Err() == nil {
		if conn == nil {
			if err := c.transport.fetchMetadata(); err != nil {
				if !cooperativeRetryable(err) {
					return err
				}
				if !cooperativeWait(ctx, retry.duration()) {
					break
				}
				continue
			}
			addr := c.transport.getPartitionLeaderAddr(pid)
			var err error
			conn, err = c.transport.client.ConnectContext(ctx, addr)
			if err != nil {
				if !cooperativeWait(ctx, retry.duration()) {
					break
				}
				continue
			}
			activeConn := conn
			stop := context.AfterFunc(ctx, func() { _ = activeConn.Close() })
			release = func() { stop() }
			response, err := c.command(ctx, fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s include_found=true", cfg.Topic, pid, cfg.GroupID), false)
			if err != nil {
				closeConn()
				if !cooperativeRetryable(err) {
					return err
				}
				if !cooperativeWait(ctx, retry.duration()) {
					break
				}
				continue
			}
			_, err = parseOKResponse(response)
			if err != nil {
				return err
			}
			stable, err := parseFetchOffsetResponse(response)
			if err != nil {
				return err
			}
			if !initialized || stable > offset {
				offset = stable
			}
			initialized = true
			if cfg.Mode == ModeStreaming {
				_ = conn.SetWriteDeadline(time.Now().Add(c.transport.requestTimeout()))
				if err := WriteWithLength(conn, []byte(c.fetchCommand("STREAM", w.token, offset))); err != nil {
					closeConn()
					if !cooperativeWait(ctx, retry.duration()) {
						break
					}
					continue
				}
			}
		}
		if cfg.Mode != ModeStreaming {
			_ = conn.SetWriteDeadline(time.Now().Add(c.transport.requestTimeout()))
			if err := WriteWithLength(conn, []byte(c.fetchCommand("CONSUME", w.token, offset))); err != nil {
				closeConn()
				if !cooperativeWait(ctx, retry.duration()) {
					break
				}
				continue
			}
		}
		timeout := time.Duration(cfg.PollTimeoutMS) * time.Millisecond
		if cfg.Mode == ModeStreaming {
			timeout = time.Duration(cfg.StreamingReadDeadlineMS) * time.Millisecond
		}
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		data, err := ReadWithLength(conn)
		if err != nil {
			closeConn()
			if !cooperativeRetryable(err) {
				return err
			}
			if !cooperativeWait(ctx, retry.duration()) {
				break
			}
			continue
		}
		if ctx.Err() != nil {
			break
		}
		if len(data) == 0 {
			continue
		}
		if frame, ok := parseStreamControlFrame(data); ok {
			closeConn()
			if frame.Reason == "offset_out_of_range" {
				return &ConsumerOffsetOutOfRangeError{Partition: pid, Requested: frame.Requested, Earliest: frame.Earliest, Latest: frame.Latest}
			}
			if !cooperativeWait(ctx, retry.duration()) {
				break
			}
			continue
		}
		messages, topic, partition, err := DecodeBatchMessages(data)
		if err != nil {
			return err
		}
		if topic != cfg.Topic || partition != pid {
			return fmt.Errorf("cooperative batch identity mismatch")
		}
		retry.reset()
		c.fetched.Add(uint64(len(messages)))
		for _, message := range messages {
			c.bytes.Add(uint64(len(message.Payload)))
		}
		if len(messages) > 0 {
			select {
			case batches <- messages:
				offset = messages[len(messages)-1].Offset + 1
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if cfg.Mode != ModeStreaming && !cooperativeWait(ctx, cfg.PollInterval) {
			break
		}
	}
	return ctx.Err()
}

func (c *CooperativeConsumer) fetchCommand(command string, token protocol.CooperativeToken, offset uint64) string {
	cfg := c.transport.config
	return fmt.Sprintf("%s topic=%s partition=%d group=%s member=%s offset=%d ownership=%s%s%s batch=%d", command, cfg.Topic, token.Grant.Partition.Partition, cfg.GroupID, token.Grant.Member, offset, protocol.EncodeCooperative(token), autoOffsetResetArgument(cfg.AutoOffsetReset), readIsolationArgument(cfg.ReadIsolation), effectivePollBatchSize(cfg))
}

func cooperativeWait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// SendCooperativeOffsets binds a transaction to the delivered input grant.
func (p *TransactionalProducer) SendCooperativeOffsets(message CooperativeMessage, nextOffset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	token := message.Ownership
	if nextOffset <= message.Offset {
		return fmt.Errorf("invalid cooperative transaction offset")
	}
	cmd, err := buildSendOffsetsToTransactionCommand(p.session.TransactionalID, p.session.ProducerID, token.Grant.Partition.Topic, message.Group, token.Grant.Member, 0, p.session.Epoch, map[int]uint64{token.Grant.Partition.Partition: nextOffset})
	if err != nil {
		return err
	}
	cmd += " ownerships=" + protocol.EncodeCooperative([]protocol.CooperativeToken{token})
	_, err = p.client.execTxnCommand(p.session.TransactionalID, cmd)
	return err
}
