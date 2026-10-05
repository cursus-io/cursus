package fsm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// PartitionTopologyHealth is a detached evaluation of one durable partition
// assignment against its topic definition and current broker registry.
type PartitionTopologyHealth struct {
	Key                string
	Topic              string
	Partition          int
	Leader             string
	LeaderEpoch        int
	Replicas           []string
	ISR                []string
	CommittedHWM       uint64
	ExpectedReplicas   int
	ActiveReplicas     int
	InactiveReplicas   int
	InSyncReplicas     int
	MinInSyncReplicas  int
	LeaderAvailable    bool
	AssignmentComplete bool
	UnderReplicated    bool
	MinISRUnsatisfied  bool
	Healthy            bool
	Reasons            []string
}

// TopologyHealth summarizes whether durable placement can satisfy the
// replication contract declared by every topic definition.
type TopologyHealth struct {
	Healthy                   bool
	PartitionCount            int
	Offline                   int
	UnderReplicated           int
	AssignmentDeficient       int
	InactiveReplicaPartitions int
	InactiveReplicas          int
	MinISRUnsatisfied         int
	Partitions                []PartitionTopologyHealth
}

// ReadinessError reports whether every partition can still serve its declared
// durability contract. A partition may remain available while a replica is
// catching up or offline, provided its assignment is intact, its leader is
// active, and its effective minimum ISR is satisfied. Full-replica health is
// exposed separately through CLUSTER_STATUS and replication metrics.
func (health TopologyHealth) ReadinessError() error {
	if health.Offline == 0 && health.AssignmentDeficient == 0 && health.MinISRUnsatisfied == 0 {
		return nil
	}
	return fmt.Errorf(
		"cluster topology unhealthy: offline=%d under_replicated=%d assignment_deficient=%d inactive_replica_partitions=%d min_isr_unsatisfied=%d",
		health.Offline,
		health.UnderReplicated,
		health.AssignmentDeficient,
		health.InactiveReplicaPartitions,
		health.MinISRUnsatisfied,
	)
}

// EvaluateTopology compares durable topic definitions with assignments, the
// broker registry, and ISR membership under one FSM read lock.
func (f *BrokerFSM) EvaluateTopology(defaultMinISR int) TopologyHealth {
	if f == nil {
		return TopologyHealth{Healthy: false}
	}
	if defaultMinISR < 1 {
		defaultMinISR = 1
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	active := make(map[string]bool, len(f.brokers))
	for brokerID, broker := range f.brokers {
		active[brokerID] = broker != nil && strings.EqualFold(broker.Status, "active")
	}

	definitions := make([]string, 0, len(f.topicState))
	for name := range f.topicState {
		definitions = append(definitions, name)
	}
	sort.Strings(definitions)

	health := TopologyHealth{Healthy: true}
	expectedKeys := make(map[string]struct{})
	for _, name := range definitions {
		definition := f.topicState[name]
		if definition == nil {
			continue
		}
		minISR := definition.Policy.EffectiveMinInSyncReplicas(defaultMinISR)
		for partition := 0; partition < definition.Partitions; partition++ {
			key := name + "-" + strconv.Itoa(partition)
			expectedKeys[key] = struct{}{}
			partitionHealth := evaluatePartitionTopology(
				key,
				name,
				partition,
				definition.ReplicationFactor,
				minISR,
				f.partitionMetadata[key],
				active,
			)
			health.addPartition(partitionHealth)
		}
	}

	strayKeys := make([]string, 0)
	for key := range f.partitionMetadata {
		if _, expected := expectedKeys[key]; !expected {
			strayKeys = append(strayKeys, key)
		}
	}
	sort.Strings(strayKeys)
	for _, key := range strayKeys {
		topicName, partition, parsed := splitPartitionMetadataKey(key)
		if !parsed {
			topicName = key
			partition = -1
		}
		partitionHealth := evaluatePartitionTopology(key, topicName, partition, 0, defaultMinISR, f.partitionMetadata[key], active)
		partitionHealth.AssignmentComplete = false
		partitionHealth.Healthy = false
		partitionHealth.Reasons = append(partitionHealth.Reasons, "missing_topic_definition")
		health.addPartition(partitionHealth)
	}

	return health
}

func evaluatePartitionTopology(
	key string,
	topicName string,
	partition int,
	expectedReplicas int,
	minISR int,
	metadata *PartitionMetadata,
	active map[string]bool,
) PartitionTopologyHealth {
	health := PartitionTopologyHealth{
		Key: key, Topic: topicName, Partition: partition,
		ExpectedReplicas:  expectedReplicas,
		MinInSyncReplicas: minISR,
		Healthy:           true,
	}
	if metadata == nil {
		health.AssignmentComplete = false
		health.UnderReplicated = expectedReplicas > 0
		health.MinISRUnsatisfied = minISR > 0
		health.Healthy = false
		health.Reasons = []string{"missing_partition_metadata"}
		if expectedReplicas > 0 {
			health.Reasons = append(health.Reasons, "replica_count_mismatch", "isr_below_replication_factor")
		}
		if minISR > 0 {
			health.Reasons = append(health.Reasons, "isr_below_minimum")
		}
		return health
	}

	health.Leader = metadata.Leader
	health.LeaderEpoch = metadata.LeaderEpoch
	health.Replicas = append([]string(nil), metadata.Replicas...)
	health.ISR = append([]string(nil), metadata.ISR...)
	health.CommittedHWM = metadata.CommittedHWM

	replicaSet := make(map[string]struct{}, len(metadata.Replicas))
	assignmentValid := true
	for _, replica := range metadata.Replicas {
		if strings.TrimSpace(replica) == "" {
			health.addReason("empty_replica")
			assignmentValid = false
			continue
		}
		if _, duplicate := replicaSet[replica]; duplicate {
			health.addReason("duplicate_replica")
			assignmentValid = false
			continue
		}
		replicaSet[replica] = struct{}{}
		isActive, known := active[replica]
		if !known {
			health.addReason("unknown_replica")
			health.InactiveReplicas++
			continue
		}
		if !isActive {
			health.addReason("inactive_replica")
			health.InactiveReplicas++
			continue
		}
		health.ActiveReplicas++
	}
	if len(replicaSet) != expectedReplicas {
		health.addReason("replica_count_mismatch")
		assignmentValid = false
	}
	health.AssignmentComplete = assignmentValid

	if metadata.Leader == "" {
		health.addReason("leader_missing")
	} else if _, configured := replicaSet[metadata.Leader]; !configured {
		health.addReason("leader_not_replica")
	} else if isActive, known := active[metadata.Leader]; !known {
		health.addReason("leader_unknown")
	} else if !isActive {
		health.addReason("leader_inactive")
	} else {
		health.LeaderAvailable = true
	}

	isrSet := make(map[string]struct{}, len(metadata.ISR))
	for _, replica := range metadata.ISR {
		if strings.TrimSpace(replica) == "" {
			health.addReason("empty_isr")
			continue
		}
		if _, duplicate := isrSet[replica]; duplicate {
			health.addReason("duplicate_isr")
			continue
		}
		isrSet[replica] = struct{}{}
		if _, configured := replicaSet[replica]; !configured {
			health.addReason("isr_not_replica")
			continue
		}
		isActive, known := active[replica]
		if !known {
			health.addReason("isr_unknown")
			continue
		}
		if !isActive {
			health.addReason("isr_inactive")
			continue
		}
		health.InSyncReplicas++
	}
	health.UnderReplicated = expectedReplicas > 0 && health.InSyncReplicas < expectedReplicas
	if health.UnderReplicated {
		health.addReason("isr_below_replication_factor")
	}
	health.MinISRUnsatisfied = health.InSyncReplicas < minISR
	if health.MinISRUnsatisfied {
		health.addReason("isr_below_minimum")
	}
	health.Healthy = len(health.Reasons) == 0
	return health
}

func (health *PartitionTopologyHealth) addReason(reason string) {
	for _, existing := range health.Reasons {
		if existing == reason {
			return
		}
	}
	health.Reasons = append(health.Reasons, reason)
}

func (health *TopologyHealth) addPartition(partition PartitionTopologyHealth) {
	health.Partitions = append(health.Partitions, partition)
	health.PartitionCount++
	if !partition.LeaderAvailable {
		health.Offline++
	}
	if partition.UnderReplicated {
		health.UnderReplicated++
	}
	if !partition.AssignmentComplete {
		health.AssignmentDeficient++
	}
	if partition.InactiveReplicas > 0 {
		health.InactiveReplicaPartitions++
		health.InactiveReplicas += partition.InactiveReplicas
	}
	if partition.MinISRUnsatisfied {
		health.MinISRUnsatisfied++
	}
	if !partition.Healthy {
		health.Healthy = false
	}
}
