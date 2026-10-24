package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type activationSource struct {
	bindingSource
	incarnation   string
	ready         bool
	consumerReady bool
}

func (s *activationSource) ConsumerRoutingRecoveryReady() bool { return s.consumerReady }

func (s *activationSource) RecoveryRuntimeReady(incarnation string) error {
	if !s.ready || s.incarnation != incarnation {
		return fsm.ErrBootstrapTransferUnavailable
	}
	return nil
}

func TestRecoveryActivationRequiresInstalledBundle(t *testing.T) {
	sd := NewServiceDiscoveryImpl(nil, "source", "source:7000", "source:9000")
	version := func() int {
		data, err := sd.LocalRegistrationPayload()
		require.NoError(t, err)
		var info fsm.BrokerInfo
		require.NoError(t, json.Unmarshal(data, &info))
		return info.RecoveryProtocol
	}
	require.Zero(t, version())
	require.Error(t, sd.ActivateRecoveryRuntime())
	source := &activationSource{incarnation: sd.incarnationID}
	require.NoError(t, sd.BindBootstrapTransferSource(source))
	require.Error(t, sd.ActivateRecoveryRuntime())
	require.Zero(t, version())
	source.ready = true
	require.NoError(t, sd.ActivateRecoveryRuntime())
	require.Equal(t, 2, version())
	require.NoError(t, sd.ActivateRecoveryRuntime())
}

func TestRegistrationPublishesAndRevokesConsumerRoutingReadiness(t *testing.T) {
	sd := NewServiceDiscoveryImpl(nil, "source", "source:7000", "source:9000")
	source := &activationSource{incarnation: sd.incarnationID, ready: true, consumerReady: true}
	require.NoError(t, sd.BindBootstrapTransferSource(source))
	read := func() fsm.BrokerInfo {
		data, err := sd.LocalRegistrationPayload()
		require.NoError(t, err)
		var info fsm.BrokerInfo
		require.NoError(t, json.Unmarshal(data, &info))
		return info
	}
	require.False(t, read().ConsumerRoutingReady, "runtime must be installed first")
	require.NoError(t, sd.ActivateRecoveryRuntime())
	require.True(t, read().ConsumerRoutingReady)
	source.consumerReady = false
	require.False(t, read().ConsumerRoutingReady, "restore or lost authority revokes the report")
}

func TestRecoverySourceRejectsRestoredProcessRegistration(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	rm := &ComprehensiveMockRaftManager{mockFSM: state}
	sd := NewServiceDiscoveryImpl(rm, "source", "source:7000", "source:9000")
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`PARTITION:orders-0:{"leader":"source","leader_epoch":1,"partition_count":1,"lifecycle_epoch":1,"committed_hwm_version":1,"committed_hwm":0}`)}))
	info := fsm.BrokerInfo{ID: "source", Addr: "source:7000", Status: "active", RecoveryProtocol: 2, IncarnationID: "old-process", LifecycleProtocol: 6}
	register := func() {
		data, err := json.Marshal(info)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	register()
	request := fsm.ReplicaCatchupRequest{Topic: "orders", SourceBroker: "source", Leader: "source", RecoveryProtocol: 2}
	_, err := sd.FetchReplicaCatchupContext(context.Background(), request)
	require.ErrorContains(t, err, "source process registration is not current")
	info.IncarnationID = sd.incarnationID
	register()
	_, err = sd.FetchReplicaCatchupContext(context.Background(), request)
	require.Error(t, err, "the deliberately incomplete request must reach FSM validation")
	require.NotContains(t, err.Error(), "source process registration is not current")
}
