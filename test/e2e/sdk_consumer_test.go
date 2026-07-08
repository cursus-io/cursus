package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	sdk "github.com/cursus-io/cursus/sdk"
)

func TestGoSDKProducerConsumerBenchmarkRoundTrip(t *testing.T) {
	sdk.SetLogLevel(sdk.LogLevelWarn)
	const (
		totalMessages = 100000
		partitions    = 12
	)

	ctx := GivenStandalone(t).
		WithTopic(fmt.Sprintf("sdk-consumer-benchmark-%d", time.Now().UnixNano())).
		WithPartitions(partitions).
		WithConsumerGroup(fmt.Sprintf("sdk-consumer-group-%d", time.Now().UnixNano()))
	defer ctx.Cleanup()

	producerCfg := sdk.NewDefaultPublisherConfig()
	producerCfg.BrokerAddrs = defaultBrokerAddrs
	producerCfg.Topic = ctx.topic
	producerCfg.EnableIdempotence = false
	producerCfg.Partitions = partitions
	producerCfg.BatchSize = 5000
	producerCfg.BufferSize = totalMessages
	producerCfg.LingerMS = 50
	producerCfg.AckTimeoutMS = 30000
	producerCfg.FlushTimeoutMS = 300000
	producerCfg.MaxRetries = 10
	producerCfg.RetryBackoffMS = 500
	producerCfg.WriteTimeoutMS = 30000
	producerCfg.LogLevel = sdk.LogLevelWarn

	producer, err := sdk.NewProducer(producerCfg)
	if err != nil {
		t.Fatalf("create sdk producer: %v", err)
	}
	for i := 0; i < totalMessages; i++ {
		if _, err := producer.Send(fmt.Sprintf("sdk-message-%03d", i)); err != nil {
			t.Fatalf("sdk send message %d: %v", i, err)
		}
	}
	producer.FlushBenchmark(totalMessages)
	if got := producer.GetUniqueAckCount(); got != totalMessages {
		t.Fatalf("sdk producer acked %d messages, want %d", got, totalMessages)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close sdk producer: %v", err)
	}

	consumer := newSDKTestConsumer(t, ctx, "sdk-consumer-1")
	seen := consumeWithSDK(t, consumer, totalMessages, 180*time.Second)
	if len(seen) != totalMessages {
		t.Fatalf("sdk consumer read %d unique messages, want %d", len(seen), totalMessages)
	}
	for i := 0; i < totalMessages; i++ {
		payload := fmt.Sprintf("sdk-message-%03d", i)
		if _, ok := seen[payload]; !ok {
			t.Fatalf("sdk consumer missing payload %s", payload)
		}
	}
	waitCommittedAtLeast(t, ctx, partitions, totalMessages, 60*time.Second)
	closeSDKConsumer(t, consumer)

	resumeConsumer := newSDKTestConsumer(t, ctx, "sdk-consumer-2")
	resumed := consumeWithSDK(t, resumeConsumer, 1, 2*time.Second)
	closeSDKConsumer(t, resumeConsumer)
	if len(resumed) != 0 {
		t.Fatalf("sdk consumer resume delivered %d already-committed messages, want 0", len(resumed))
	}
}

func waitCommittedAtLeast(t *testing.T, ctx *TestContext, partitions int, want uint64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastTotal uint64
	for time.Now().Before(deadline) {
		var total uint64
		for partition := 0; partition < partitions; partition++ {
			offset, err := ctx.GetClient().FetchCommittedOffset(ctx.topic, partition, ctx.consumerGroup)
			if err != nil {
				t.Fatalf("fetch committed offset for partition %d: %v", partition, err)
			}
			total += offset
		}
		if total >= want {
			return
		}
		lastTotal = total
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("committed offsets sum = %d, want at least %d", lastTotal, want)
}

func newSDKTestConsumer(t *testing.T, ctx *TestContext, id string) *sdk.Consumer {
	t.Helper()

	cfg := sdk.NewDefaultConsumerConfig()
	cfg.BrokerAddrs = defaultBrokerAddrs
	cfg.Topic = ctx.topic
	cfg.GroupID = ctx.consumerGroup
	cfg.ConsumerID = id
	cfg.Mode = sdk.ModePolling
	cfg.BatchSize = 5000
	cfg.MaxPollRecords = 5000
	cfg.PollInterval = 25 * time.Millisecond
	cfg.PollTimeoutMS = 10000
	cfg.SessionTimeoutMS = 60000
	cfg.HeartbeatIntervalMS = 2000
	cfg.AutoCommitInterval = 200 * time.Millisecond
	cfg.WorkerChannelSize = 8192
	cfg.LogLevel = sdk.LogLevelWarn

	consumer, err := sdk.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("create sdk consumer: %v", err)
	}
	return consumer
}

func consumeWithSDK(t *testing.T, consumer *sdk.Consumer, target int, timeout time.Duration) map[string]struct{} {
	t.Helper()

	done := make(chan struct{})
	startErr := make(chan error, 1)
	seen := make(map[string]struct{})
	var mu sync.Mutex
	var once sync.Once

	go func() {
		startErr <- consumer.Start(func(msg sdk.Message) error {
			mu.Lock()
			seen[msg.Payload] = struct{}{}
			if len(seen) >= target {
				once.Do(func() { close(done) })
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case <-done:
	case err := <-startErr:
		if err != nil {
			t.Fatalf("sdk consumer start: %v", err)
		}
	case <-time.After(timeout):
	}

	mu.Lock()
	defer mu.Unlock()
	result := make(map[string]struct{}, len(seen))
	for payload := range seen {
		result[payload] = struct{}{}
	}
	return result
}

func closeSDKConsumer(t *testing.T, consumer *sdk.Consumer) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close sdk consumer: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("close sdk consumer timed out")
	}
}
