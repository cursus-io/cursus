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
		partition, err := consumerRoutePartition(group, root.PartitionCount)
		if err != nil {
			return nil, nil, err
		}
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
	expectedPartition, err := consumerRoutePartition(route.Group, route.PartitionCount)
	if root == nil || meta == nil || root.PartitionCount != route.PartitionCount || meta.PartitionCount != route.PartitionCount ||
		root.LifecycleEpoch != route.LifecycleEpoch || meta.LifecycleEpoch != route.LifecycleEpoch || meta.Leader != route.Owner || meta.LeaderEpoch != route.LeaderEpoch ||
		err != nil || expectedPartition != partition {
		return fmt.Errorf("%w: stale consumer metadata route", ErrPartitionCommitFenced)
	}
	return nil
}

func consumerRoutePartition(group string, count int) (int, error) {
	unsignedCount, ok := util.SafeIntToUint64(count)
	if !ok || unsignedCount == 0 {
		return 0, fmt.Errorf("invalid consumer metadata partition count %d", count)
	}
	partition, ok := util.SafeUint64ToInt(util.GenerateID(coordinator.ConsumerMetadataGroupPartitionKey(group)) % unsignedCount)
	if !ok {
		return 0, fmt.Errorf("consumer metadata partition out of range")
	}
	return partition, nil
}
