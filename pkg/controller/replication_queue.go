package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

var errReplicationQueueClosed = errors.New("replication queue closed")

type partitionReplicationTask struct {
	topic        string
	partition    int
	command      types.MessageCommand
	commitHWM    uint64
	ackMode      ackpolicy.Mode
	barrierOnly  bool
	snapshot     clusterController.PartitionReplicationSnapshot
	partitionRef *topic.Partition
	result       chan error
}

type partitionReplicationExecutor interface {
	Snapshot(topic string, partition int) (clusterController.PartitionReplicationSnapshot, error)
	ReplicateISR(ctx context.Context, task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error
	ReplicateNonISR(task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error
	Commit(task partitionReplicationTask) error
}

type clusterPartitionReplicationExecutor struct {
	handler *CommandHandler
}

func (e clusterPartitionReplicationExecutor) Snapshot(topicName string, partition int) (clusterController.PartitionReplicationSnapshot, error) {
	return e.handler.Cluster.GetPartitionReplicationSnapshot(topicName, partition)
}

func (e clusterPartitionReplicationExecutor) ReplicateISR(ctx context.Context, task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.handler.Cluster.ReplicateToISR(task.topic, task.partition, task.command, snapshot)
}

func (e clusterPartitionReplicationExecutor) ReplicateNonISR(task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) error {
	commitHWM := task.commitHWM
	command := task.command
	command.CommitHWM = &commitHWM
	return e.handler.Cluster.ReplicateToNonISR(task.topic, task.partition, command, snapshot)
}

func (e clusterPartitionReplicationExecutor) Commit(task partitionReplicationTask) error {
	if err := e.handler.commitPartitionHWMAtEpoch(
		task.topic,
		task.partition,
		task.commitHWM,
		task.snapshot.Leader,
		task.snapshot.LeaderEpoch,
	); err != nil {
		return err
	}
	if err := task.partitionRef.ApplyReplicaHWM(task.commitHWM); err != nil {
		return fmt.Errorf("apply local commit watermark: %w", err)
	}
	task.partitionRef.FlushDisk()
	return nil
}

type partitionReplicationCoordinator struct {
	ctx            context.Context
	cancel         context.CancelFunc
	reserveCtx     context.Context
	cancelReserves context.CancelFunc
	capacity       int
	executor       partitionReplicationExecutor
	mu             sync.Mutex
	closed         bool
	lanes          map[string]*partitionReplicationLane
	submissions    sync.WaitGroup
	workers        sync.WaitGroup
}

type partitionReplicationLane struct {
	owner   *partitionReplicationCoordinator
	queue   chan partitionReplicationTask
	catchup chan partitionCatchupTask
	slots   chan struct{}
}

type partitionCatchupTask struct {
	task     partitionReplicationTask
	snapshot clusterController.PartitionReplicationSnapshot
}

type partitionReplicationReservation struct {
	lane *partitionReplicationLane
	once sync.Once
}

func newPartitionReplicationCoordinator(capacity int, executor partitionReplicationExecutor) *partitionReplicationCoordinator {
	if capacity <= 0 {
		capacity = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	reserveCtx, cancelReserves := context.WithCancel(context.Background())
	return &partitionReplicationCoordinator{
		ctx:            ctx,
		cancel:         cancel,
		reserveCtx:     reserveCtx,
		cancelReserves: cancelReserves,
		capacity:       capacity,
		executor:       executor,
		lanes:          make(map[string]*partitionReplicationLane),
	}
}

func (c *partitionReplicationCoordinator) reserve(ctx context.Context, topicName string, partition int) (*partitionReplicationReservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errReplicationQueueClosed
	}
	key := fmt.Sprintf("%s-%d", topicName, partition)
	lane := c.lanes[key]
	if lane == nil {
		lane = &partitionReplicationLane{
			owner:   c,
			queue:   make(chan partitionReplicationTask, c.capacity),
			catchup: make(chan partitionCatchupTask, c.capacity),
			slots:   make(chan struct{}, c.capacity),
		}
		c.lanes[key] = lane
		c.workers.Add(2)
		go lane.run()
		go lane.runCatchup()
	}
	c.submissions.Add(1)
	c.mu.Unlock()

	select {
	case lane.slots <- struct{}{}:
		return &partitionReplicationReservation{lane: lane}, nil
	case <-ctx.Done():
		c.submissions.Done()
		return nil, ctx.Err()
	case <-c.reserveCtx.Done():
		c.submissions.Done()
		return nil, errReplicationQueueClosed
	}
}

func (r *partitionReplicationReservation) submit(task partitionReplicationTask) {
	r.once.Do(func() {
		r.lane.queue <- task
		r.lane.owner.submissions.Done()
	})
}

func (r *partitionReplicationReservation) release() {
	if r == nil || r.lane == nil {
		return
	}
	r.once.Do(func() {
		<-r.lane.slots
		r.lane.owner.submissions.Done()
	})
}

func (c *partitionReplicationCoordinator) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	c.cancelReserves()
	c.submissions.Wait()
	c.cancel()
	c.workers.Wait()
}

func (l *partitionReplicationLane) run() {
	defer l.owner.workers.Done()
	for {
		select {
		case task := <-l.queue:
			if l.owner.ctx.Err() != nil {
				completeReplicationTask(task, errReplicationQueueClosed)
				<-l.slots
				continue
			}
			l.process(task)
			<-l.slots
		case <-l.owner.ctx.Done():
			for {
				select {
				case task := <-l.queue:
					completeReplicationTask(task, errReplicationQueueClosed)
					<-l.slots
				default:
					return
				}
			}
		}
	}
}

func (l *partitionReplicationLane) runCatchup() {
	defer l.owner.workers.Done()
	for {
		if l.owner.ctx.Err() != nil {
			return
		}
		select {
		case catchup := <-l.catchup:
			if err := l.owner.executor.ReplicateNonISR(catchup.task, catchup.snapshot); err != nil {
				class := replicationErrorClass(err)
				metrics.AsyncReplicationFailures.WithLabelValues(catchup.task.topic, class).Inc()
				util.Error("async non-ISR replication failed topic=%s partition=%d ack_mode=%s error_class=%s error=%v", catchup.task.topic, catchup.task.partition, catchup.task.ackMode, class, err)
			}
		case <-l.owner.ctx.Done():
			return
		}
	}
}

func (l *partitionReplicationLane) enqueueCatchup(task partitionReplicationTask, snapshot clusterController.PartitionReplicationSnapshot) {
	select {
	case l.catchup <- partitionCatchupTask{task: task, snapshot: snapshot}:
	default:
		metrics.AsyncReplicationFailures.WithLabelValues(task.topic, "backpressure").Inc()
		util.Error("async non-ISR replication queue full topic=%s partition=%d ack_mode=%s error_class=backpressure", task.topic, task.partition, task.ackMode)
	}
}

func (l *partitionReplicationLane) process(task partitionReplicationTask) {
	backoff := 25 * time.Millisecond
	reported := false
	for {
		if l.owner.ctx.Err() != nil {
			completeReplicationTask(task, errReplicationQueueClosed)
			return
		}

		snapshot, err := l.replicationSnapshot(task)
		if err == nil && task.barrierOnly {
			completeReplicationTask(task, nil)
			return
		}
		if err == nil {
			err = l.owner.executor.ReplicateISR(l.owner.ctx, task, snapshot)
		}
		if err == nil {
			current, snapshotErr := l.owner.executor.Snapshot(task.topic, task.partition)
			if snapshotErr != nil {
				err = snapshotErr
			} else if current.Leader != task.snapshot.Leader || current.LeaderEpoch != task.snapshot.LeaderEpoch {
				err = fmt.Errorf("%w before commit: current=%s/%d requested=%s/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch)
			}
		}
		if err == nil {
			err = l.owner.executor.Commit(task)
		}
		if err == nil {
			current, snapshotErr := l.owner.executor.Snapshot(task.topic, task.partition)
			if snapshotErr != nil {
				err = snapshotErr
			} else if current.Leader != task.snapshot.Leader || current.LeaderEpoch != task.snapshot.LeaderEpoch {
				err = fmt.Errorf("%w after commit: current=%s/%d requested=%s/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch)
			}
		}
		if err == nil {
			completeReplicationTask(task, nil)
			l.enqueueCatchup(task, snapshot)
			return
		}
		if l.owner.ctx.Err() != nil {
			completeReplicationTask(task, errReplicationQueueClosed)
			return
		}

		if !reported {
			reported = true
			class := replicationErrorClass(err)
			if task.ackMode == ackpolicy.All {
				completeReplicationTask(task, err)
			} else {
				metrics.AsyncReplicationFailures.WithLabelValues(task.topic, class).Inc()
				util.Error("async replication failed topic=%s partition=%d ack_mode=%s error_class=%s error=%v", task.topic, task.partition, task.ackMode, class, err)
			}
		}
		if isReplicationFenceError(err) {
			completeReplicationTask(task, err)
			return
		}
		select {
		case <-time.After(backoff):
			if backoff < time.Second {
				backoff *= 2
			}
		case <-l.owner.ctx.Done():
			completeReplicationTask(task, errReplicationQueueClosed)
			return
		}
	}
}

func (l *partitionReplicationLane) replicationSnapshot(task partitionReplicationTask) (clusterController.PartitionReplicationSnapshot, error) {
	current, err := l.owner.executor.Snapshot(task.topic, task.partition)
	if err != nil {
		return clusterController.PartitionReplicationSnapshot{}, err
	}
	if current.Leader != task.snapshot.Leader || current.LeaderEpoch != task.snapshot.LeaderEpoch {
		return clusterController.PartitionReplicationSnapshot{}, fmt.Errorf("%w: current=%s/%d requested=%s/%d", clusterController.ErrPartitionLeaderFenced, current.Leader, current.LeaderEpoch, task.snapshot.Leader, task.snapshot.LeaderEpoch)
	}
	if task.ackMode == ackpolicy.All {
		return task.snapshot, nil
	}
	return current, nil
}

func completeReplicationTask(task partitionReplicationTask, err error) {
	if task.result == nil {
		return
	}
	select {
	case task.result <- err:
	default:
	}
}

func isReplicationFenceError(err error) bool {
	return errors.Is(err, clusterController.ErrPartitionLeaderFenced)
}

func replicationErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	value := strings.ToLower(err.Error())
	switch {
	case isReplicationFenceError(err):
		return "fenced"
	case strings.Contains(value, "in-sync") || strings.Contains(value, "isr"):
		return "insufficient_isr"
	case strings.Contains(value, "timeout") || strings.Contains(value, "deadline"):
		return "timeout"
	case strings.Contains(value, "cancel"):
		return "cancelled"
	case errors.Is(err, errReplicationQueueClosed):
		return "shutdown"
	default:
		return "replication"
	}
}
