package observability

import (
	"testing"

	clustercontroller "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fixedTopics struct{ snapshot topic.RuntimeSnapshot }

func (source fixedTopics) RuntimeSnapshot() topic.RuntimeSnapshot { return source.snapshot }

type fixedGroups struct {
	snapshot map[string]*coordinator.GroupStateSnapshot
}

func (source fixedGroups) ExportState() map[string]*coordinator.GroupStateSnapshot {
	return source.snapshot
}

type fixedDisk struct{ snapshot disk.RuntimeSnapshot }

func (source fixedDisk) RuntimeSnapshot() disk.RuntimeSnapshot { return source.snapshot }

type fixedStreams int

func (source fixedStreams) ActiveCount() int { return int(source) }

type fixedCluster struct {
	snapshot clustercontroller.RuntimeSnapshot
}

func (source fixedCluster) RuntimeSnapshot() clustercontroller.RuntimeSnapshot {
	return source.snapshot
}

type fixedReadiness bool

func (source fixedReadiness) IsReady() bool { return bool(source) }

func TestCollectorExportsScrapeTimeBrokerState(t *testing.T) {
	collector := NewCollector(
		fixedTopics{snapshot: topic.RuntimeSnapshot{
			TopicCount:                      1,
			MetadataLoadFailure:             "decode failed",
			MetadataOrphanTopicCount:        3,
			MetadataDurabilityWarning:       "directory sync failed",
			MetadataDurabilityWarningsTotal: 7,
			Partitions: []topic.PartitionRuntimeSnapshot{{
				Topic: "orders", Partition: 0, LogStart: 2, LogEnd: 10, HighWatermark: 8,
			}},
		}},
		fixedGroups{snapshot: map[string]*coordinator.GroupStateSnapshot{
			"workers": {
				TopicName: "orders", Generation: 3,
				Members: map[string][]int{"member-1": {0}},
				Offsets: map[string]map[int]uint64{"orders": {0: 5}},
			},
			"new-workers": {TopicName: "orders", Members: map[string][]int{}, Offsets: map[string]map[int]uint64{}},
			"ahead-workers": {
				TopicName: "orders", Members: map[string][]int{},
				Offsets: map[string]map[int]uint64{"orders": {0: 9}},
			},
		}},
		fixedDisk{snapshot: disk.RuntimeSnapshot{Handlers: 1, Segments: 2, Bytes: 4096, PendingWrites: 4, ActiveReaders: 2}},
		fixedStreams(2),
		fixedCluster{snapshot: clustercontroller.RuntimeSnapshot{
			Enabled: true, BrokerCount: 3, HasLeader: true, IsLeader: true, UnderReplicated: 1,
			PartitionDetails: []clustercontroller.PartitionRuntimeSnapshot{{
				Topic: "orders", Partition: 0, Leader: "broker-1", LeaderEpoch: 4, Replicas: 3, InSync: 2,
			}},
		}},
		fixedReadiness(true),
	)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	assertGauge(t, families, "cursus_broker_ready", nil, 1)
	assertGauge(t, families, "cursus_topic_metadata_manifest_load_failure", nil, 1)
	assertGauge(t, families, "cursus_topic_metadata_orphan_topics", nil, 3)
	assertGauge(t, families, "cursus_topic_metadata_durability_warning", nil, 1)
	assertCounter(t, families, "cursus_topic_metadata_durability_warnings_total", 7)
	assertGauge(t, families, "cursus_partition_high_watermark", map[string]string{"topic": "orders", "partition": "0"}, 8)
	assertGauge(t, families, "cursus_consumer_group_committed_offset", map[string]string{"group": "workers", "topic": "orders", "partition": "0"}, 5)
	assertGauge(t, families, "cursus_consumer_group_lag", map[string]string{"group": "workers", "topic": "orders", "partition": "0"}, 3)
	assertGauge(t, families, "cursus_consumer_group_lag", map[string]string{"group": "new-workers", "topic": "orders", "partition": "0"}, 8)
	assertGauge(t, families, "cursus_consumer_group_offset_out_of_range", map[string]string{"group": "new-workers", "topic": "orders", "partition": "0"}, 1)
	assertGauge(t, families, "cursus_consumer_group_offset_out_of_range", map[string]string{"group": "ahead-workers", "topic": "orders", "partition": "0"}, 1)
	assertGauge(t, families, "cursus_storage_bytes", nil, 4096)
	assertGauge(t, families, "cursus_streams_active", nil, 2)
	assertGauge(t, families, "cursus_cluster_under_replicated_partitions", nil, 1)
	assertGauge(t, families, "cursus_cluster_partition_leader", map[string]string{"topic": "orders", "partition": "0", "broker_id": "broker-1"}, 1)
}

func assertGauge(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if labelsMatch(metric.Label, labels) {
				if got := metric.GetGauge().GetValue(); got != want {
					t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("metric %s%v not found", name, labels)
}

func assertCounter(t *testing.T, families []*dto.MetricFamily, name string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name && family.GetType() == dto.MetricType_COUNTER && len(family.Metric) == 1 {
			if got := family.Metric[0].GetCounter().GetValue(); got != want {
				t.Fatalf("%s = %v, want %v", name, got, want)
			}
			return
		}
	}
	t.Fatalf("counter %s not found", name)
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, pair := range pairs {
		if want[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}
