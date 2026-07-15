package observability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	clustercontroller "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/prometheus/client_golang/prometheus"
)

type topicSource interface {
	RuntimeSnapshot() topic.RuntimeSnapshot
}

type groupSource interface {
	ExportState() map[string]*coordinator.GroupStateSnapshot
}

type diskSource interface {
	RuntimeSnapshot() disk.RuntimeSnapshot
}

type streamSource interface {
	ActiveCount() int
}

type clusterSource interface {
	RuntimeSnapshot() clustercontroller.RuntimeSnapshot
}

// ReadinessSource reports whether the broker is ready to accept client work.
type ReadinessSource interface {
	IsReady() bool
}

// Collector exports scrape-time state rather than retaining stale gauge labels.
type Collector struct {
	topics      topicSource
	groups      groupSource
	disk        diskSource
	streams     streamSource
	cluster     clusterSource
	readiness   ReadinessSource
	descriptors []*prometheus.Desc

	ready                  *prometheus.Desc
	topicCount             *prometheus.Desc
	partitionCount         *prometheus.Desc
	logStart               *prometheus.Desc
	logEnd                 *prometheus.Desc
	highWatermark          *prometheus.Desc
	groupMembers           *prometheus.Desc
	groupGeneration        *prometheus.Desc
	groupAssignments       *prometheus.Desc
	groupCommittedOffset   *prometheus.Desc
	groupLag               *prometheus.Desc
	legacyGroupLag         *prometheus.Desc
	groupOffsetOutOfRange  *prometheus.Desc
	activeStreams          *prometheus.Desc
	storageHandlers        *prometheus.Desc
	storageSegments        *prometheus.Desc
	storageBytes           *prometheus.Desc
	storagePendingWrites   *prometheus.Desc
	storageActiveReaders   *prometheus.Desc
	storageStatFailures    *prometheus.Desc
	distributionEnabled    *prometheus.Desc
	clusterBrokers         *prometheus.Desc
	clusterHasLeader       *prometheus.Desc
	clusterIsLeader        *prometheus.Desc
	clusterOffline         *prometheus.Desc
	clusterUnderReplicated *prometheus.Desc
	partitionReplicas      *prometheus.Desc
	partitionInSync        *prometheus.Desc
	partitionLeaderEpoch   *prometheus.Desc
	partitionLeader        *prometheus.Desc
}

// NewCollector creates a broker runtime collector. Nil sources are supported.
func NewCollector(topics topicSource, groups groupSource, diskState diskSource, streams streamSource, cluster clusterSource, readiness ReadinessSource) *Collector {
	c := &Collector{
		topics:                 topics,
		groups:                 groups,
		disk:                   diskState,
		streams:                streams,
		cluster:                cluster,
		readiness:              readiness,
		ready:                  prometheus.NewDesc("cursus_broker_ready", "Whether the broker is ready to accept client work.", nil, nil),
		topicCount:             prometheus.NewDesc("cursus_broker_topics", "Number of topics loaded by this broker.", nil, nil),
		partitionCount:         prometheus.NewDesc("cursus_broker_partitions", "Number of topic partitions loaded by this broker.", nil, nil),
		logStart:               prometheus.NewDesc("cursus_partition_log_start_offset", "Earliest retained offset for a partition.", []string{"topic", "partition"}, nil),
		logEnd:                 prometheus.NewDesc("cursus_partition_log_end_offset", "Next offset allocated in a partition.", []string{"topic", "partition"}, nil),
		highWatermark:          prometheus.NewDesc("cursus_partition_high_watermark", "Next offset visible to committed readers.", []string{"topic", "partition"}, nil),
		groupMembers:           prometheus.NewDesc("cursus_consumer_group_members", "Active members in a consumer group.", []string{"group", "topic"}, nil),
		groupGeneration:        prometheus.NewDesc("cursus_consumer_group_generation", "Current consumer group generation.", []string{"group", "topic"}, nil),
		groupAssignments:       prometheus.NewDesc("cursus_consumer_group_assigned_partitions", "Partitions assigned to active group members.", []string{"group", "topic"}, nil),
		groupCommittedOffset:   prometheus.NewDesc("cursus_consumer_group_committed_offset", "Committed next offset for a consumer group partition.", []string{"group", "topic", "partition"}, nil),
		groupLag:               prometheus.NewDesc("cursus_consumer_group_lag", "Committed-reader lag, max(high watermark - committed next offset, 0).", []string{"group", "topic", "partition"}, nil),
		legacyGroupLag:         prometheus.NewDesc("broker_consumer_lag", "Deprecated alias of cursus_consumer_group_lag.", []string{"topic", "partition", "group"}, nil),
		groupOffsetOutOfRange:  prometheus.NewDesc("cursus_consumer_group_offset_out_of_range", "Whether a committed offset is outside the retained committed-readable range.", []string{"group", "topic", "partition"}, nil),
		activeStreams:          prometheus.NewDesc("cursus_streams_active", "Currently registered streaming consumers.", nil, nil),
		storageHandlers:        prometheus.NewDesc("cursus_storage_handlers", "Open partition storage handlers.", nil, nil),
		storageSegments:        prometheus.NewDesc("cursus_storage_segments", "Open storage segments including active segments.", nil, nil),
		storageBytes:           prometheus.NewDesc("cursus_storage_bytes", "Bytes used by segment and offset index files for open handlers.", nil, nil),
		storagePendingWrites:   prometheus.NewDesc("cursus_storage_pending_writes", "Messages waiting in storage write queues.", nil, nil),
		storageActiveReaders:   prometheus.NewDesc("cursus_storage_active_readers", "Readers currently accessing storage segments.", nil, nil),
		storageStatFailures:    prometheus.NewDesc("cursus_storage_stat_failures", "Storage files that could not be inspected during this scrape.", nil, nil),
		distributionEnabled:    prometheus.NewDesc("cursus_distribution_enabled", "Whether distributed cluster mode is enabled.", nil, nil),
		clusterBrokers:         prometheus.NewDesc("cursus_cluster_brokers", "Brokers present in replicated cluster metadata.", nil, nil),
		clusterHasLeader:       prometheus.NewDesc("cursus_cluster_has_leader", "Whether this broker can resolve the cluster leader.", nil, nil),
		clusterIsLeader:        prometheus.NewDesc("cursus_cluster_is_leader", "Whether this broker is the current cluster leader.", nil, nil),
		clusterOffline:         prometheus.NewDesc("cursus_cluster_offline_partitions", "Partitions without an assigned leader.", nil, nil),
		clusterUnderReplicated: prometheus.NewDesc("cursus_cluster_under_replicated_partitions", "Partitions whose in-sync replica count is below their replica count.", nil, nil),
		partitionReplicas:      prometheus.NewDesc("cursus_cluster_partition_replicas", "Configured replica count for a partition.", []string{"topic", "partition"}, nil),
		partitionInSync:        prometheus.NewDesc("cursus_cluster_partition_in_sync_replicas", "In-sync replica count for a partition.", []string{"topic", "partition"}, nil),
		partitionLeaderEpoch:   prometheus.NewDesc("cursus_cluster_partition_leader_epoch", "Current partition leader epoch.", []string{"topic", "partition"}, nil),
		partitionLeader:        prometheus.NewDesc("cursus_cluster_partition_leader", "Current partition leader identity.", []string{"topic", "partition", "broker_id"}, nil),
	}
	c.descriptors = []*prometheus.Desc{
		c.ready, c.topicCount, c.partitionCount, c.logStart, c.logEnd, c.highWatermark,
		c.groupMembers, c.groupGeneration, c.groupAssignments, c.groupCommittedOffset,
		c.groupLag, c.legacyGroupLag, c.groupOffsetOutOfRange, c.activeStreams, c.storageHandlers,
		c.storageSegments, c.storageBytes, c.storagePendingWrites, c.storageActiveReaders,
		c.storageStatFailures, c.distributionEnabled, c.clusterBrokers, c.clusterHasLeader,
		c.clusterIsLeader, c.clusterOffline, c.clusterUnderReplicated, c.partitionReplicas,
		c.partitionInSync, c.partitionLeaderEpoch, c.partitionLeader,
	}
	return c
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, descriptor := range c.descriptors {
		ch <- descriptor
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ready := false
	if c.readiness != nil {
		ready = c.readiness.IsReady()
	}
	ch <- gauge(c.ready, boolValue(ready))

	topicState := topic.RuntimeSnapshot{}
	if c.topics != nil {
		topicState = c.topics.RuntimeSnapshot()
	}
	ch <- gauge(c.topicCount, float64(topicState.TopicCount))
	ch <- gauge(c.partitionCount, float64(len(topicState.Partitions)))
	partitionState := make(map[string]topic.PartitionRuntimeSnapshot, len(topicState.Partitions))
	for _, partition := range topicState.Partitions {
		partitionLabel := strconv.Itoa(partition.Partition)
		partitionState[partitionKey(partition.Topic, partition.Partition)] = partition
		ch <- gauge(c.logStart, float64(partition.LogStart), partition.Topic, partitionLabel)
		ch <- gauge(c.logEnd, float64(partition.LogEnd), partition.Topic, partitionLabel)
		ch <- gauge(c.highWatermark, float64(partition.HighWatermark), partition.Topic, partitionLabel)
	}
	c.collectGroups(ch, partitionState)

	activeStreams := 0
	if c.streams != nil {
		activeStreams = c.streams.ActiveCount()
	}
	ch <- gauge(c.activeStreams, float64(activeStreams))
	c.collectStorage(ch)
	c.collectCluster(ch)
}

func (c *Collector) collectGroups(ch chan<- prometheus.Metric, partitions map[string]topic.PartitionRuntimeSnapshot) {
	if c.groups == nil {
		return
	}
	groups := c.groups.ExportState()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		state := groups[name]
		if state == nil {
			continue
		}
		assignments := 0
		for _, memberAssignments := range state.Members {
			assignments += len(memberAssignments)
		}
		ch <- gauge(c.groupMembers, float64(len(state.Members)), name, state.TopicName)
		ch <- gauge(c.groupGeneration, float64(state.Generation), name, state.TopicName)
		ch <- gauge(c.groupAssignments, float64(assignments), name, state.TopicName)

		positions := make(map[string]uint64)
		if !strings.ContainsAny(state.TopicName, "*?") {
			prefix := state.TopicName + "\x00"
			for key := range partitions {
				if strings.HasPrefix(key, prefix) {
					positions[key] = 0
				}
			}
		}
		for topicName, offsets := range state.Offsets {
			for partition, offset := range offsets {
				positions[partitionKey(topicName, partition)] = offset
			}
		}

		keys := make([]string, 0, len(positions))
		for key := range positions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			partition, exists := partitions[key]
			if !exists {
				continue
			}
			committed := positions[key]
			lag := uint64(0)
			if partition.HighWatermark > committed {
				lag = partition.HighWatermark - committed
			}
			outOfRange := committed < partition.LogStart || committed > partition.HighWatermark
			partitionLabel := strconv.Itoa(partition.Partition)
			ch <- gauge(c.groupCommittedOffset, float64(committed), name, partition.Topic, partitionLabel)
			ch <- gauge(c.groupLag, float64(lag), name, partition.Topic, partitionLabel)
			ch <- gauge(c.legacyGroupLag, float64(lag), partition.Topic, partitionLabel, name)
			ch <- gauge(c.groupOffsetOutOfRange, boolValue(outOfRange), name, partition.Topic, partitionLabel)
		}
	}
}

func (c *Collector) collectStorage(ch chan<- prometheus.Metric) {
	state := disk.RuntimeSnapshot{}
	if c.disk != nil {
		state = c.disk.RuntimeSnapshot()
	}
	ch <- gauge(c.storageHandlers, float64(state.Handlers))
	ch <- gauge(c.storageSegments, float64(state.Segments))
	ch <- gauge(c.storageBytes, float64(state.Bytes))
	ch <- gauge(c.storagePendingWrites, float64(state.PendingWrites))
	ch <- gauge(c.storageActiveReaders, float64(state.ActiveReaders))
	ch <- gauge(c.storageStatFailures, float64(state.StatFailures))
}

func (c *Collector) collectCluster(ch chan<- prometheus.Metric) {
	state := clustercontroller.RuntimeSnapshot{}
	if c.cluster != nil {
		state = c.cluster.RuntimeSnapshot()
	}
	ch <- gauge(c.distributionEnabled, boolValue(state.Enabled))
	ch <- gauge(c.clusterBrokers, float64(state.BrokerCount))
	ch <- gauge(c.clusterHasLeader, boolValue(state.HasLeader))
	ch <- gauge(c.clusterIsLeader, boolValue(state.IsLeader))
	ch <- gauge(c.clusterOffline, float64(state.Offline))
	ch <- gauge(c.clusterUnderReplicated, float64(state.UnderReplicated))
	for _, partition := range state.PartitionDetails {
		partitionLabel := strconv.Itoa(partition.Partition)
		ch <- gauge(c.partitionReplicas, float64(partition.Replicas), partition.Topic, partitionLabel)
		ch <- gauge(c.partitionInSync, float64(partition.InSync), partition.Topic, partitionLabel)
		ch <- gauge(c.partitionLeaderEpoch, float64(partition.LeaderEpoch), partition.Topic, partitionLabel)
		if partition.Leader != "" {
			ch <- gauge(c.partitionLeader, 1, partition.Topic, partitionLabel, partition.Leader)
		}
	}
}

func partitionKey(topicName string, partition int) string {
	return fmt.Sprintf("%s\x00%d", topicName, partition)
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}
