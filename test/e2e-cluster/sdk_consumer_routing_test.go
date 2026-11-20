package e2e_cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSDKConsumerCoordinatorRoutingAcrossThreeBrokerCluster(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithClusterSize(3).
		WithTopic("sdk-consumer-routing").
		WithPartitions(1)
	defer ctx.Cleanup()
	ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()

	t.Run("bootstrap", func(t *testing.T) {
		registry := runSDKConsumerRoutingScenario(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), "sdk-routing-bootstrap", nil)
		require.Positive(t, sdkCoordinatorMetric(t, registry,
			"cursus_consumer_coordinator_resolution_total",
			map[string]string{"result": "success", "reason": "initial", "client_role": "consumer"},
		))
	})

	for _, scenario := range []struct {
		name       string
		redirect   string
		wantLookup bool
	}{
		{name: "different-address-redirect", redirect: "different"},
		{name: "same-address-redirect", redirect: "same", wantLookup: true},
		{name: "addressless-redirect", redirect: "addressless", wantLookup: true},
	} {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			group := "sdk-routing-" + scenario.redirect
			coordinator := findSDKCoordinator(t, ctx.GetBrokerAddrs(), group)
			proxy := newSDKRoutingProxy(t)
			proxy.enqueueOK(findCoordinatorResponse(proxy.address()))
			switch scenario.redirect {
			case "different":
				proxy.enqueueRedirect(coordinator)
			case "same":
				proxy.enqueueRedirect(proxy.address())
				proxy.enqueueOK(findCoordinatorResponse(coordinator))
			case "addressless":
				proxy.enqueueRedirect("")
				proxy.enqueueOK(findCoordinatorResponse(coordinator))
			}

			brokers := append([]string{proxy.address()}, ctx.GetBrokerAddrs()...)
			registry := runSDKConsumerRoutingScenario(t, brokers, ctx.GetTopic(), group, proxy)

			require.Positive(t, sdkCoordinatorMetric(t, registry,
				"cursus_consumer_coordinator_redirects_total",
				map[string]string{"command": "join_group", "client_role": "consumer", "route_source": "cache"},
			))
			require.Positive(t, sdkCoordinatorMetric(t, registry,
				"cursus_consumer_coordinator_cache_refresh_total",
				map[string]string{"result": "success", "reason": "redirect", "client_role": "consumer"},
			))
			require.Positive(t, sdkCoordinatorMetric(t, registry,
				"cursus_consumer_coordinator_retry_total",
				map[string]string{"command": "join_group", "result": "success", "client_role": "consumer"},
			))
			if scenario.wantLookup {
				require.Positive(t, sdkCoordinatorMetric(t, registry,
					"cursus_consumer_coordinator_resolution_total",
					map[string]string{"result": "success", "reason": "redirect", "client_role": "consumer"},
				))
			}
		})
	}
}

func runSDKConsumerRoutingScenario(
	t *testing.T,
	brokers []string,
	topic, group string,
	proxy *sdkRoutingProxy,
) *prometheus.Registry {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics, err := sdk.NewMetrics(registry, registry)
	require.NoError(t, err)

	cfg := sdk.NewDefaultConsumerConfig()
	cfg.BrokerAddrs = brokers
	cfg.Topic = topic
	cfg.GroupID = group
	cfg.ConsumerID = group + "-member"
	cfg.Metrics = metrics
	cfg.PollInterval = time.Hour
	cfg.RequestTimeoutMS = 5000

	consumer, err := sdk.NewConsumer(cfg)
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- consumer.Start(func(sdk.Message) error { return nil }) }()

	require.NoError(t, eventually(t, "SDK consumer membership readiness", 20*time.Second, func() (bool, string, error) {
		snapshot := consumer.Membership()
		return snapshot.Ready, fmt.Sprintf("state=%s membership=%+v", consumer.State(), snapshot), consumer.Err()
	}))
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, consumer.CloseContext(closeCtx))
	require.NoError(t, <-started)
	if proxy != nil {
		proxy.requireNoError(t)
	}
	return registry
}

func findSDKCoordinator(t *testing.T, brokers []string, group string) string {
	t.Helper()
	client := e2e.NewBrokerClient(brokers)
	defer client.Close()
	response, err := client.SendCommand("", "FIND_COORDINATOR group="+group, 5*time.Second)
	require.NoError(t, err)
	fields := parseSDKRoutingFields(response)
	host, port := fields["host"], fields["port"]
	require.NotEmpty(t, host)
	require.NotEmpty(t, port)
	return net.JoinHostPort(host, port)
}

func findCoordinatorResponse(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("OK host=%s port=%s", host, port)
}

func parseSDKRoutingFields(response string) map[string]string {
	fields := make(map[string]string)
	for _, field := range strings.Fields(response) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			fields[key] = value
		}
	}
	return fields
}

type sdkRoutingStep struct {
	response string
	redirect string
}

type sdkRoutingProxy struct {
	listener net.Listener
	mu       sync.Mutex
	steps    []sdkRoutingStep
	err      error
}

func newSDKRoutingProxy(t *testing.T) *sdkRoutingProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &sdkRoutingProxy{listener: listener}
	go proxy.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return proxy
}

func (p *sdkRoutingProxy) address() string { return p.listener.Addr().String() }

func (p *sdkRoutingProxy) enqueueOK(response string) {
	p.mu.Lock()
	p.steps = append(p.steps, sdkRoutingStep{response: response})
	p.mu.Unlock()
}

func (p *sdkRoutingProxy) enqueueRedirect(address string) {
	p.mu.Lock()
	p.steps = append(p.steps, sdkRoutingStep{redirect: address})
	p.mu.Unlock()
}

func (p *sdkRoutingProxy) nextStep() (sdkRoutingStep, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.steps) == 0 {
		return sdkRoutingStep{}, false
	}
	step := p.steps[0]
	p.steps = p.steps[1:]
	return step, true
}

func (p *sdkRoutingProxy) serve() {
	for {
		raw, err := p.listener.Accept()
		if err != nil {
			return
		}
		step, ok := p.nextStep()
		if !ok {
			// The consumer client may probe its most recent bootstrap route once
			// more before failing over for metadata. The routing script is already
			// complete, so close that probe like an unavailable bootstrap broker.
			_ = raw.Close()
			continue
		}
		if err := p.respond(raw, step); err != nil {
			p.setError(err)
		}
	}
}

func (p *sdkRoutingProxy) respond(raw net.Conn, step sdkRoutingStep) error {
	defer func() { _ = raw.Close() }()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	connection, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := connection.ReadFrame()
	if err != nil {
		return err
	}
	if step.redirect != "" || step.response == "" {
		fields := map[string]string{}
		if step.redirect != "" {
			host, portText, splitErr := net.SplitHostPort(step.redirect)
			if splitErr != nil {
				return splitErr
			}
			if _, parseErr := strconv.Atoi(portText); parseErr != nil {
				return parseErr
			}
			fields["host"] = host
			fields["port"] = portText
		}
		payload, encodeErr := wire.EncodeError(wire.ErrorPayload{
			Code: "NOT_COORDINATOR", Class: wire.ErrorClassRouting, Retryable: true, Fields: fields,
		})
		if encodeErr != nil {
			return encodeErr
		}
		return connection.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusError,
			RequestID: request.RequestID, Payload: payload,
		})
	}
	return connection.WriteFrame(wire.Frame{
		Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK,
		RequestID: request.RequestID, Payload: []byte(step.response),
	})
}

func (p *sdkRoutingProxy) setError(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}

func (p *sdkRoutingProxy) requireNoError(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NoError(t, p.err)
	require.Empty(t, p.steps, "routing proxy did not receive every scripted command")
}

func sdkCoordinatorMetric(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if sdkRoutingLabelsMatch(metric.Label, labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func sdkRoutingLabelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
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
