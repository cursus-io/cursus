package controller

// PartitionRuntimeSnapshot describes replicated partition placement.
type PartitionRuntimeSnapshot struct {
	Topic             string
	Partition         int
	Leader            string
	LeaderEpoch       int
	Replicas          int
	ExpectedReplicas  int
	ActiveReplicas    int
	InactiveReplicas  int
	InSync            int
	MinInSyncReplicas int
	Healthy           bool
}

// MaterializationAttemptsSnapshot counts local convergence attempts by result.
type MaterializationAttemptsSnapshot struct {
	Success uint64
	Failure uint64
}

// RuntimeSnapshot is a point-in-time view of cluster control state.
type RuntimeSnapshot struct {
	Enabled                           bool
	BrokerID                          string
	BrokerCount                       int
	HasLeader                         bool
	IsLeader                          bool
	Offline                           int
	UnderReplicated                   int
	AssignmentDeficient               int
	InactiveReplicaPartitions         int
	InactiveReplicas                  int
	MinISRUnsatisfied                 int
	TopicMaterializationsPending      map[string]int
	TopicMaterializationAttempts      map[string]MaterializationAttemptsSnapshot
	TopicMaterializationOldestPending float64
	PartitionDetails                  []PartitionRuntimeSnapshot
}

// RuntimeSnapshot returns cluster metadata without exposing mutable FSM state.
func (cc *ClusterController) RuntimeSnapshot() RuntimeSnapshot {
	if cc == nil || cc.RaftManager == nil {
		return RuntimeSnapshot{}
	}

	snapshot := RuntimeSnapshot{
		Enabled:   true,
		BrokerID:  cc.brokerID,
		HasLeader: cc.RaftManager.GetLeaderAddress() != "",
		IsLeader:  cc.RaftManager.IsLeader(),
	}
	fsmState := cc.RaftManager.GetFSM()
	if fsmState == nil {
		return snapshot
	}

	snapshot.BrokerCount = len(fsmState.GetBrokers())
	materialization := fsmState.TopicMaterializationRuntimeSnapshot()
	snapshot.TopicMaterializationsPending = make(map[string]int, len(materialization.PendingByOperation))
	for operation, count := range materialization.PendingByOperation {
		snapshot.TopicMaterializationsPending[operation] = count
	}
	snapshot.TopicMaterializationAttempts = make(map[string]MaterializationAttemptsSnapshot, len(materialization.AttemptsByOperation))
	for operation, attempts := range materialization.AttemptsByOperation {
		snapshot.TopicMaterializationAttempts[operation] = MaterializationAttemptsSnapshot{
			Success: attempts.Success,
			Failure: attempts.Failure,
		}
	}
	snapshot.TopicMaterializationOldestPending = materialization.OldestPending.Seconds()
	defaultMinISR := 1
	if cc.Config != nil {
		defaultMinISR = cc.Config.MinInSyncReplicas
	}
	topology := fsmState.EvaluateTopology(defaultMinISR)
	snapshot.Offline = topology.Offline
	snapshot.UnderReplicated = topology.UnderReplicated
	snapshot.AssignmentDeficient = topology.AssignmentDeficient
	snapshot.InactiveReplicaPartitions = topology.InactiveReplicaPartitions
	snapshot.InactiveReplicas = topology.InactiveReplicas
	snapshot.MinISRUnsatisfied = topology.MinISRUnsatisfied
	for _, partition := range topology.Partitions {
		detail := PartitionRuntimeSnapshot{
			Topic: partition.Topic, Partition: partition.Partition,
			Leader: partition.Leader, LeaderEpoch: partition.LeaderEpoch,
			Replicas: len(partition.Replicas), ExpectedReplicas: partition.ExpectedReplicas,
			ActiveReplicas: partition.ActiveReplicas, InactiveReplicas: partition.InactiveReplicas,
			InSync:            partition.InSyncReplicas,
			MinInSyncReplicas: partition.MinInSyncReplicas, Healthy: partition.Healthy,
		}
		snapshot.PartitionDetails = append(snapshot.PartitionDetails, detail)
	}

	return snapshot
}
