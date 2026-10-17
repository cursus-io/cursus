package fsm

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// ConsumerMetadataRoutes reads the partition mapping and its owners under one
// metadata lock. A route cannot combine the count and leader of different views.
func (f *BrokerFSM) ConsumerMetadataRoutes(groups []string) (map[string]types.ConsumerMetadataRoute, map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	root := f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"]
	if root == nil || root.PartitionCount <= 0 {
		return nil, nil, fmt.Errorf("consumer offsets topology unavailable")
	}
	routes := make(map[string]types.ConsumerMetadataRoute, len(groups))
	addresses := make(map[string]string, len(groups))
	for _, group := range groups {
		partition := int(util.GenerateID(coordinator.ConsumerMetadataGroupPartitionKey(group)) % uint64(root.PartitionCount))
		meta := f.partitionMetadata[fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, partition)]
		if meta == nil || meta.Leader == "" || meta.PartitionCount != root.PartitionCount || meta.LifecycleEpoch != root.LifecycleEpoch {
			return nil, nil, fmt.Errorf("consumer offsets partition %d has no consistent durable route", partition)
		}
		broker := f.brokers[meta.Leader]
		if broker == nil || broker.Status != "active" {
			return nil, nil, fmt.Errorf("consumer offsets partition %d leader %s is not active", partition, meta.Leader)
		}
		routes[group] = types.ConsumerMetadataRoute{Group: group, Owner: meta.Leader, LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch, PartitionCount: root.PartitionCount, Partition: partition}
		addresses[group] = broker.Addr
	}
	return routes, addresses, nil
}

func (f *BrokerFSM) ValidateConsumerMetadataRoute(topic string, partition int, route *types.ConsumerMetadataRoute) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.validateConsumerMetadataRouteLocked(topic, partition, route)
}

func (f *BrokerFSM) validateConsumerMetadataRouteLocked(topic string, partition int, route *types.ConsumerMetadataRoute) error {
	// Legacy writes remain compatible until durable routing activation.
	if route == nil {
		return nil
	}
	if topic != config.ConsumerOffsetsTopicName || route.PartitionCount <= 0 || route.Partition != partition || route.Owner == "" || route.LeaderEpoch <= 0 {
		return fmt.Errorf("%w: invalid consumer metadata route", ErrPartitionCommitFenced)
	}
	root := f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"]
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", topic, partition)]
	if root == nil || meta == nil || root.PartitionCount != route.PartitionCount || meta.PartitionCount != route.PartitionCount ||
		root.LifecycleEpoch != route.LifecycleEpoch || meta.LifecycleEpoch != route.LifecycleEpoch || meta.Leader != route.Owner || meta.LeaderEpoch != route.LeaderEpoch ||
		int(util.GenerateID(coordinator.ConsumerMetadataGroupPartitionKey(route.Group))%uint64(route.PartitionCount)) != partition {
		return fmt.Errorf("%w: stale consumer metadata route", ErrPartitionCommitFenced)
	}
	return nil
}
