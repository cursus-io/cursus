package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	composeFile               = "docker-compose.yml"
	envOnce                   sync.Once
	defaultBrokerAddrs        = []string{"localhost:10000"}
	StandAloneHealthCheckAddr = []string{"http://localhost:10080/health"}
)

// TestContext holds all test state and configuration
type TestContext struct {
	t              *testing.T
	brokerAddrs    []string
	topic          string
	partitions     int
	numMessages    int
	publishDelayMS int

	// Test state
	startTime      time.Time
	publishedCount int
	consumedCount  int
	consumerGroup  string
	lastError      error

	// Producer state
	producerID       string
	seqNum           uint64
	publishedSeqNums []uint64
	acks             string

	// Consumer state
	memberID           string
	generation         int
	assignedPartitions []int
	offsetCache        map[int]uint64

	// Client helper
	client *BrokerClient
}

// Given creates a new test context with default values
func Given(t *testing.T) *TestContext {
	uniqueID := uuid.New().String()[:8]
	return &TestContext{
		t:              t,
		brokerAddrs:    defaultBrokerAddrs,
		topic:          "test-topic",
		partitions:     1,
		numMessages:    10,
		publishDelayMS: 0,
		startTime:      time.Now(),
		consumerGroup:  fmt.Sprintf("test-group-%s", uniqueID),
		memberID:       fmt.Sprintf("e2e-consumer-%s", uniqueID),
		generation:     0,
		producerID:     uuid.New().String(),
		seqNum:         0,
		acks:           "1",
		client:         nil, // lazily initialized
	}
}

func getComposeCommand() []string {
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}
	}
	return []string{"docker", "compose"}
}

func RunCompose(args ...string) *exec.Cmd {
	base := getComposeCommand()
	fullArgs := append(base[1:], args...)
	return exec.Command(base[0], fullArgs...)
}

// GivenRestart starts the broker environment and returns a new context
func initEnvironment(t *testing.T) {
	envOnce.Do(func() {
		t.Log("Starting docker compose environment...")

		cmd := RunCompose("-f", composeFile, "up", "-d")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Failed to start docker compose: %v\nOutput: %s", err, string(output))
		}

		if err := CheckBrokerHealth(StandAloneHealthCheckAddr); err != nil {
			t.Fatalf("Broker failed to become healthy: %v", err)
		}
	})
}

func GivenStandalone(t *testing.T) *TestContext {
	initEnvironment(t)
	return Given(t)
}

func TestMain(m *testing.M) {
	code := m.Run()

	fmt.Println("All tests finished. Cleaning up docker compose environment...")
	cmd := RunCompose("-f", composeFile, "down", "-v")

	if err := cmd.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: docker compose down failed: %v\n", err)
	}

	os.Exit(code)
}

func (ctx *TestContext) GetT() *testing.T {
	return ctx.t
}

func (ctx *TestContext) getClient() *BrokerClient {
	if ctx.client == nil {
		ctx.client = NewBrokerClient(ctx.brokerAddrs)
		ctx.client.SetMemberID(ctx.memberID)
		ctx.t.Logf("Initialized BrokerClient with nodes: %v", ctx.brokerAddrs)
	}

	return ctx.client
}

// GetNumMessages returns the current number of messages setting
func (ctx *TestContext) GetNumMessages() int {
	return ctx.numMessages
}

// SetNumMessages sets the number of messages (for temporary modification)
func (ctx *TestContext) SetNumMessages(num int) *TestContext {
	ctx.numMessages = num
	return ctx
}

func (ctx *TestContext) SetBrokerAddrs(addrs []string) {
	ctx.brokerAddrs = addrs
	if ctx.client != nil {
		ctx.client.Close()
		ctx.client = nil
	}
}

// GetBrokerAddr returns the broker address
func (ctx *TestContext) GetBrokerAddrs() []string {
	return ctx.brokerAddrs
}

// GetTopic returns the topic name
func (ctx *TestContext) GetTopic() string {
	return ctx.topic
}

// GetPartitions returns the number of partitions
func (ctx *TestContext) GetPartitions() int {
	return ctx.partitions
}

// GetConsumerGroup returns the consumer group name
func (ctx *TestContext) GetConsumerGroup() string {
	return ctx.consumerGroup
}

// GetPublishedCount returns the number of published messages
func (ctx *TestContext) GetPublishedCount() int {
	return ctx.publishedCount
}

func (ctx *TestContext) ResetPublishedCount() *TestContext {
	ctx.publishedCount = 0
	ctx.publishedSeqNums = []uint64{}
	return ctx
}

// GetConsumedCount returns the number of consumed messages
func (ctx *TestContext) GetConsumedCount() int {
	return ctx.consumedCount
}

// GetLastError returns the last error encountered during test execution
func (ctx *TestContext) GetLastError() error {
	return ctx.lastError
}

// GetAcks returns the acks setting
func (ctx *TestContext) GetAcks() string {
	return ctx.acks
}

// GetClient returns the broker client
func (ctx *TestContext) GetClient() *BrokerClient {
	return ctx.getClient()
}

// GetAssignedPartitions returns the assigned partitions
func (ctx *TestContext) GetAssignedPartitions() []int {
	return ctx.assignedPartitions
}

// SetConsumedCount sets the consumed message count
func (ctx *TestContext) SetConsumedCount(count int) {
	ctx.consumedCount = count
}

// SetLastError records the last error encountered
func (ctx *TestContext) SetLastError(err error) {
	ctx.lastError = err
}

// GetProducerID returns the current producer ID
func (c *TestContext) GetProducerID() string {
	return c.producerID
}

// GetPublishedSeqNums returns the sequence numbers of published messages
func (c *TestContext) GetPublishedSeqNums() []uint64 {
	return c.publishedSeqNums
}

// Configuration methods (fluent interface)
func (ctx *TestContext) WithTopic(topic string) *TestContext {
	ctx.topic = topic

	if ctx.client != nil {
		ctx.client.Close()
		ctx.client = nil
	}
	return ctx
}

func (ctx *TestContext) WithPartitions(partitions int) *TestContext {
	ctx.partitions = partitions
	return ctx
}

func (ctx *TestContext) WithNumMessages(num int) *TestContext {
	ctx.numMessages = num
	return ctx
}

func (ctx *TestContext) WithPublishDelay(delayMS int) *TestContext {
	ctx.publishDelayMS = delayMS
	return ctx
}

func (ctx *TestContext) WithAcks(acks string) *TestContext {
	ctx.acks = acks
	return ctx
}

func (ctx *TestContext) WithConsumerGroup(group string) *TestContext {
	ctx.consumerGroup = group
	return ctx
}

// When returns Actions for test execution
func (ctx *TestContext) When() *Actions {
	return &Actions{ctx: ctx}
}

// Then returns Consequences for assertions
func (ctx *TestContext) Then() *Consequences {
	return &Consequences{ctx: ctx}
}

// Cleanup stops broker and cleans up resources
func (ctx *TestContext) Cleanup() {
	ctx.t.Log("Cleaning up test resources...")

	if ctx.topic != "" {
		if err := ctx.getClient().DeleteTopic(ctx.topic); err != nil {
			if !strings.Contains(err.Error(), "not found") {
				ctx.t.Logf("Failed to delete topic %s: %v", ctx.topic, err)
			}
		} else {
			ctx.t.Logf("Topic %s deleted", ctx.topic)
		}
	}

	if ctx.client != nil {
		ctx.client.Close()
	}

	time.Sleep(1 * time.Second)
}

// SyncClientState updates the TestContext with the latest group state from the BrokerClient
func (ctx *TestContext) SyncClientState(client *BrokerClient) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.memberID != "" && client.memberID != ctx.memberID {
		ctx.t.Logf("Context updated: memberID changed from '%s' to '%s'", ctx.memberID, client.memberID)
		ctx.memberID = client.memberID
		ctx.generation = client.generation
	}
}
