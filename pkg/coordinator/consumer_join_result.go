package coordinator

// ConsumerJoinResult is captured from the durable lifecycle candidate before
// releasing admission. A later maintenance/recovery lease must not turn an
// acknowledged JOIN into a successful response with a fabricated generation 0.
type ConsumerJoinResult struct {
	Generation       int
	Assignments      []int
	TopicAssignments []TopicPartition
}

func (c *Coordinator) AddConsumerWithResult(groupName, consumerID string) (ConsumerJoinResult, error) {
	var result ConsumerJoinResult
	release, err := c.admitConsumerOperation()
	if err != nil {
		return result, err
	}
	defer release()
	_, err = c.addConsumerResult(groupName, consumerID, publishLifecycle, &result)
	return result, err
}
