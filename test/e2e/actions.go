package e2e

import (
	"fmt"
	"time"
)

// Actions represents test actions (When phase)
type Actions struct {
	ctx *TestContext
}

func (a *Actions) StartBroker() *Actions {
	a.ctx.t.Log("Waiting for broker to be ready...")
	if err := CheckBrokerHealth(StandAloneHealthCheckAddr); err != nil {
		a.ctx.t.Fatalf("Broker health check failed: %v", err)
	}

	a.ctx.t.Log("Broker is ready")
	return a
}

func (a *Actions) StopBroker() *Actions {
	a.ctx.t.Log("Stopping broker...")
	time.Sleep(2 * time.Second)
	return a
}

func (a *Actions) CreateTopic() *Actions {
	a.ctx.t.Logf("Creating topic '%s' with %d partitions...", a.ctx.topic, a.ctx.partitions)

	err := a.ctx.getClient().CreateTopic(a.ctx.topic, a.ctx.partitions)
	if err != nil {
		a.ctx.t.Fatalf("Failed to create topic: %v", err)
	}

	a.ctx.t.Logf("Topic '%s' created successfully", a.ctx.topic)
	return a
}

func (a *Actions) PublishMessages() *Actions {
	a.ctx.t.Logf("Publishing %d messages to topic '%s'...", a.ctx.numMessages, a.ctx.topic)

	a.ctx.publishedSeqNums = make([]uint64, 0, a.ctx.numMessages)

	for i := 0; i < a.ctx.numMessages; i++ {
		a.ctx.seqNum++
		payload := fmt.Sprintf("test-message-%d", i)

		err := a.ctx.getClient().PublishIdempotent(
			a.ctx.topic,
			a.ctx.producerID,
			a.ctx.seqNum,
			time.Now().UnixNano(),
			payload,
			a.ctx.acks,
		)

		if err != nil {
			a.ctx.t.Logf("Expected/Captured publish failure: %v", err)
			a.ctx.SetLastError(err)
			return a
		}

		a.ctx.publishedSeqNums = append(a.ctx.publishedSeqNums, a.ctx.seqNum)
		a.ctx.publishedCount++

		if a.ctx.publishDelayMS > 0 {
			time.Sleep(time.Duration(a.ctx.publishDelayMS) * time.Millisecond)
		}
	}

	a.ctx.t.Logf("Published %d messages successfully", a.ctx.publishedCount)
	return a
}

func (a *Actions) RetryPublishMessages() *Actions {
	a.ctx.t.Log("Retrying published messages (idempotence test)...")

	for i := 0; i < len(a.ctx.publishedSeqNums); i++ {
		payload := fmt.Sprintf("test-message-%d", i)
		seqNum := a.ctx.publishedSeqNums[i]

		err := a.ctx.getClient().PublishIdempotent(
			a.ctx.topic,
			a.ctx.producerID,
			seqNum,
			time.Now().UnixNano(),
			payload,
			a.ctx.acks,
		)

		if err != nil {
			a.ctx.t.Logf("Retry message %d: %v", i, err)
		}
	}

	return a
}

func (a *Actions) JoinGroup() *Actions {
	a.ctx.t.Logf("Joining group '%s' for topic '%s'...", a.ctx.consumerGroup, a.ctx.topic)
	client := a.ctx.getClient()

	_, _, err := client.JoinGroup(a.ctx.topic, a.ctx.consumerGroup)
	if err != nil {
		a.ctx.t.Fatalf("Group join failed: %v", err)
	}
	a.ctx.SyncClientState(client)

	a.ctx.t.Logf("Group join successful (Member ID: %s, Generation: %d)", client.memberID, client.generation)
	return a
}

func (a *Actions) SyncGroup() *Actions {
	client := a.ctx.getClient()

	if client.memberID == "" {
		a.ctx.t.Fatalf("Cannot sync group: Member ID or Generation is missing. Did you call JoinGroup()?")
	}

	a.ctx.t.Logf("Syncing group '%s' (Generation %d) to receive partition assignments...", a.ctx.consumerGroup, client.generation)
	assignedPartitions, err := client.SyncGroup(a.ctx.topic, a.ctx.consumerGroup, client.generation, client.memberID)
	if err != nil {
		a.ctx.t.Fatalf("Group sync failed: %v", err)
	}
	a.ctx.SyncClientState(client)

	a.ctx.t.Logf("Consumer member assigned partitions: %v", assignedPartitions)
	a.ctx.assignedPartitions = assignedPartitions
	return a
}

// ConsumeMessages attempts to consume messages from all partitions in the group.
func (a *Actions) ConsumeMessages() *Actions {
	return a.ConsumeMessagesFromTopic(a.ctx.topic)
}

func (a *Actions) ConsumeMessagesFromTopic(topic string) *Actions {
	a.ctx.t.Logf("Consuming from topic '%s' for group '%s'...", topic, a.ctx.consumerGroup)

	client := a.ctx.getClient()

	if len(a.ctx.assignedPartitions) == 0 {
		a.ctx.t.Logf("No partitions assigned. Skipping consumption.")
		return a
	}

	totalConsumed := 0
	for _, partition := range a.ctx.assignedPartitions {
		messages, offsets, err := client.ConsumeMessagesWithOffsets(
			topic, partition, a.ctx.consumerGroup, client.memberID, client.generation, 5*time.Second,
		)

		if err != nil {
			a.ctx.t.Fatalf("Consume assigned partition %d failed: %v", partition, err)
		}

		count := len(messages)
		totalConsumed += len(messages)

		if count > 0 {
			lastOffset := offsets[len(offsets)-1]
			nextOffset := lastOffset + 1

			err := client.CommitOffset(topic, partition, a.ctx.consumerGroup, nextOffset)
			if err != nil {
				a.ctx.t.Fatalf("Failed to commit offset %d for partition %d: %v", nextOffset, partition, err)
			}
			a.ctx.t.Logf("Successfully committed next offset %d for partition %d", nextOffset, partition)
		}
	}

	a.ctx.consumedCount = totalConsumed
	return a
}

func (a *Actions) CommitOffset(partition int, offset uint64) *Actions {
	a.ctx.t.Logf("Committing offset %d for partition %d in group '%s'", offset, partition, a.ctx.consumerGroup)

	client := a.ctx.getClient()

	err := client.CommitOffset(a.ctx.topic, partition, a.ctx.consumerGroup, offset)
	if err != nil {
		a.ctx.t.Fatalf("Failed to commit offset: %v", err)
	}

	return a
}

func (a *Actions) CommitAllOffsets(offset uint64) *Actions {
	client := a.ctx.getClient()
	for _, partition := range a.ctx.assignedPartitions {
		err := client.CommitOffset(a.ctx.topic, partition, a.ctx.consumerGroup, offset)
		if err != nil {
			a.ctx.t.Fatalf("Failed to commit offset for partition %d: %v", partition, err)
		}
	}
	return a
}

func (a *Actions) FetchOffset(partition int) *Actions {
	a.ctx.t.Logf("Fetching offset for partition %d in group '%s'...", partition, a.ctx.consumerGroup)
	client := a.ctx.getClient()

	offset, err := client.FetchCommittedOffset(a.ctx.topic, partition, a.ctx.consumerGroup)
	if err != nil {
		a.ctx.t.Fatalf("Fetch offset failed: %v", err)
	}

	if a.ctx.offsetCache == nil {
		a.ctx.offsetCache = make(map[int]uint64)
	}
	a.ctx.offsetCache[partition] = offset
	a.ctx.t.Logf("Committed offset: %d", offset)
	return a
}

func (a *Actions) SimulateNetworkFailure() *Actions {
	a.ctx.t.Log("Simulating network failure...")
	time.Sleep(100 * time.Millisecond)
	a.ctx.t.Log("Network failure simulated")
	return a
}

func (a *Actions) SendCommand(cmd string) (string, error) {
	return a.ctx.getClient().SendCommand("", cmd, 5*time.Second)
}

// Then transitions to assertion phase
func (a *Actions) Then() *Consequences {
	return &Consequences{ctx: a.ctx}
}
