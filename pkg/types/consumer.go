package types

import "time"

type ConsumerState int

const (
	ConsumerStateActive ConsumerState = iota
	ConsumerStateDead
	ConsumerStateRebalancing
)

// Consumer represents a single consumer instance in a group.
type Consumer struct {
	ID                 int
	LastHeartbeat      time.Time
	AssignedPartitions []int
	State              ConsumerState
}

// ConsumerGroup contains consumers subscribed to the same topic.
type ConsumerGroup struct {
	Name      string
	Consumers []*Consumer
}
