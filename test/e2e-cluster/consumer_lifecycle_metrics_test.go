package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestConsumerLifecycleMetricsAcrossThreeBrokerCluster(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("consumer-lifecycle-metrics").
		WithPartitions(1)
	defer ctx.Cleanup()
	ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()

	groupName := "consumer-lifecycle-observers"
	first := e2e.NewBrokerClient(clusterBrokerAddrs(3))
	t.Cleanup(first.Close)
	firstGeneration, firstMember, err := first.JoinGroup(ctx.GetTopic(), groupName)
	require.NoError(t, err)
	joined := waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 1, "stable", clusterReadyTimeout)
	require.Positive(t, joined.lastActivity)
	require.Positive(t, joined.lastRebalance)

	second := e2e.NewBrokerClient(clusterBrokerAddrs(3))
	t.Cleanup(second.Close)
	secondGeneration, secondMember, err := second.JoinGroup(ctx.GetTopic(), groupName)
	require.NoError(t, err)
	waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 2, "stable", clusterReadyTimeout)
	require.NoError(t, leaveLifecycleGroup(second, ctx.GetTopic(), groupName, secondMember, secondGeneration))
	waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 1, "stable", clusterReadyTimeout)

	currentGeneration := readLifecycleGroupGeneration(t, first, groupName)
	require.Greater(t, currentGeneration, firstGeneration)
	require.NoError(t, leaveLifecycleGroup(first, ctx.GetTopic(), groupName, firstMember, currentGeneration))
	waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 0, "empty", clusterReadyTimeout)
	assertBrokerReadiness(t, []int{1, 2, 3})

	canary := e2e.NewBrokerClient(clusterBrokerAddrs(3))
	t.Cleanup(canary.Close)
	canaryGeneration, canaryMember, err := canary.JoinGroup(ctx.GetTopic(), groupName)
	require.NoError(t, err)
	rejoined := waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 1, "stable", clusterReadyTimeout)
	oldOwner := rejoined.owner
	require.Contains(t, []int{1, 2, 3}, oldOwner)

	deregisterLifecycleCoordinator(t, oldOwner)
	moved := waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 1, "stable", clusterFailureWait)
	require.NotEqual(t, oldOwner, moved.owner)

	resume := e2e.NewBrokerClient(clusterBrokerAddrs(3))
	t.Cleanup(resume.Close)
	response, err := resume.SendCommand("", fmt.Sprintf(
		"JOIN_GROUP topic=%s group=%s member=%s generation=%d",
		ctx.GetTopic(), groupName, canaryMember, canaryGeneration,
	), 5*time.Second)
	require.NoError(t, err)
	require.Contains(t, response, "resumed=true")
	waitForLifecycleAggregate(t, []int{1, 2, 3}, ctx.GetTopic(), groupName, 1, "stable", clusterFailureWait)
	assertBrokerReadiness(t, []int{1, 2, 3})
}

type lifecycleAggregate struct {
	coordinatorSum float64
	memberSeries   int
	members        float64
	state          float64
	lastActivity   float64
	lastRebalance  float64
	owner          int
}

func waitForLifecycleAggregate(
	t *testing.T,
	nodes []int,
	topicName, groupName string,
	wantMembers int,
	wantState string,
	timeout time.Duration,
) lifecycleAggregate {
	t.Helper()
	var observed lifecycleAggregate
	err := eventually(t, fmt.Sprintf("consumer lifecycle %s/%s members=%d", topicName, groupName, wantMembers), timeout, func() (bool, string, error) {
		aggregate, err := scrapeLifecycleAggregate(nodes, topicName, groupName, wantState)
		observed = aggregate
		if err != nil {
			return false, "metrics unavailable", err
		}
		ready := aggregate.coordinatorSum == 1 &&
			aggregate.memberSeries == 1 &&
			aggregate.members == float64(wantMembers) &&
			aggregate.state == 1 &&
			aggregate.lastActivity > 0 &&
			aggregate.lastRebalance > 0
		detail := fmt.Sprintf("coordinator_sum=%v member_series=%d members=%v state=%v owner=%d", aggregate.coordinatorSum, aggregate.memberSeries, aggregate.members, aggregate.state, aggregate.owner)
		return ready, detail, nil
	})
	require.NoError(t, err)
	return observed
}

func scrapeLifecycleAggregate(nodes []int, topicName, groupName, state string) (lifecycleAggregate, error) {
	aggregate := lifecycleAggregate{}
	for _, node := range nodes {
		families, err := scrapeBrokerMetricFamilies(node)
		if err != nil {
			return aggregate, err
		}
		labels := map[string]string{"topic": topicName, "group": groupName}
		up, found := lifecycleGauge(families, "cursus_consumer_group_coordinator_up", labels)
		if !found {
			return aggregate, fmt.Errorf("broker-%d coordinator metric missing", node)
		}
		aggregate.coordinatorSum += up
		if up == 1 {
			aggregate.owner = node
		}
		if members, ok := lifecycleGauge(families, "cursus_consumer_group_members", labels); ok {
			aggregate.memberSeries++
			if members > aggregate.members || aggregate.memberSeries == 1 {
				aggregate.members = members
			}
		}
		stateLabels := map[string]string{"topic": topicName, "group": groupName, "state": state}
		if value, ok := lifecycleGauge(families, "cursus_consumer_group_state", stateLabels); ok && value > aggregate.state {
			aggregate.state = value
		}
		if value, ok := lifecycleGauge(families, "cursus_consumer_group_last_activity_timestamp_seconds", labels); ok && value > aggregate.lastActivity {
			aggregate.lastActivity = value
		}
		if value, ok := lifecycleGauge(families, "cursus_consumer_group_last_rebalance_timestamp_seconds", labels); ok && value > aggregate.lastRebalance {
			aggregate.lastRebalance = value
		}
	}
	return aggregate, nil
}

func scrapeBrokerMetricFamilies(node int) (map[string]*dto.MetricFamily, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+node)
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(url) // #nosec G107 -- fixed loopback test endpoint.
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broker-%d metrics status %d", node, response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	return parser.TextToMetricFamilies(response.Body)
}

func lifecycleGauge(families map[string]*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	family := families[name]
	if family == nil {
		return 0, false
	}
	for _, metric := range family.Metric {
		if lifecycleLabelsMatch(metric.Label, labels) {
			return metric.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

func lifecycleLabelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
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

func leaveLifecycleGroup(client *e2e.BrokerClient, topicName, groupName, memberID string, generation int) error {
	response, err := client.SendCommand("", fmt.Sprintf(
		"LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
		topicName, groupName, memberID, generation,
	), 5*time.Second)
	if err != nil {
		return err
	}
	if len(response) < 2 || response[:2] != "OK" {
		return fmt.Errorf("leave group response: %s", response)
	}
	return nil
}

func readLifecycleGroupGeneration(t *testing.T, client *e2e.BrokerClient, groupName string) int {
	t.Helper()
	response, err := client.SendCommand("", "GROUP_STATUS group="+groupName, 5*time.Second)
	require.NoError(t, err)
	var status struct {
		Generation int `json:"generation"`
	}
	require.NoError(t, json.Unmarshal([]byte(response), &status))
	return status.Generation
}

func assertBrokerReadiness(t *testing.T, nodes []int) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	for _, node := range nodes {
		response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", healthPort(node))) // #nosec G107 -- fixed loopback test endpoint.
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		_ = response.Body.Close()
	}
}

func deregisterLifecycleCoordinator(t *testing.T, node int) {
	t.Helper()
	payload := fmt.Sprintf(`{"id":"broker-%d-9000"}`, node)
	command := fmt.Sprintf(
		"RAFT_APPLY internal_token=cursus-test-internal-token type=DEREGISTER payload=%s",
		payload,
	)
	var failures []string
	for broker := 1; broker <= 3; broker++ {
		client := e2e.NewBrokerClient([]string{fmt.Sprintf("127.0.0.1:%d", 19000+broker)})
		response, err := client.SendCommand("", command, 5*time.Second)
		client.Close()
		if err == nil && strings.HasPrefix(response, "OK") {
			return
		}
		failures = append(failures, fmt.Sprintf("broker-%d response=%q err=%v", broker, response, err))
	}
	t.Fatalf("failed to deregister coordinator broker-%d: %s", node, strings.Join(failures, "; "))
}
