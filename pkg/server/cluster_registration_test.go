package server

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

type registrationCaptureRaftManager struct {
	controller.RaftManager
	state   *fsm.BrokerFSM
	payload []byte
}

func (m *registrationCaptureRaftManager) GetFSM() *fsm.BrokerFSM { return m.state }

func (m *registrationCaptureRaftManager) ApplyCommand(command string, data []byte) error {
	if command != "REGISTER" {
		return errors.New("unexpected command")
	}
	m.payload = append([]byte(nil), data...)
	return errors.New("not leader")
}

func TestForwardedLocalRegistrationUsesDirectPayload(t *testing.T) {
	rm := &registrationCaptureRaftManager{state: fsm.NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)}
	sd := controller.NewServiceDiscoveryImpl(rm, "node-1", "raft:9001", "client:9000")
	require.ErrorContains(t, sd.Register(), "not leader")
	cfg := config.DefaultConfig()
	cfg.InternalAuthToken = "registration-test-token"
	command, err := forwardedLocalRegistrationCommand(sd, cfg)
	require.NoError(t, err)
	prefix, payload, ok := strings.Cut(command, "payload=")
	require.True(t, ok)
	require.Equal(t, "RAFT_APPLY "+internalAuthPrefix(cfg)+"type=REGISTER ", prefix)
	var direct, forwarded fsm.BrokerInfo
	require.NoError(t, json.Unmarshal(rm.payload, &direct))
	require.NoError(t, json.Unmarshal([]byte(payload), &forwarded))
	require.Zero(t, direct.RecoveryProtocol)
	require.Zero(t, forwarded.RecoveryProtocol)
	require.Equal(t, fsm.BrokerProtocolVersionCurrent, forwarded.LifecycleProtocol)
	require.Equal(t, "node-1", forwarded.ID)
	require.Equal(t, "raft:9001", forwarded.Addr)
	require.Equal(t, "client:9000", forwarded.ClientAddr)
	require.Equal(t, sd.BrokerIncarnationID(), forwarded.IncarnationID)
	require.NotEmpty(t, forwarded.IncarnationID)
	require.Equal(t, 7, forwarded.TransactionCoordinatorShards)
	require.Equal(t, "active", forwarded.Status)
	require.False(t, direct.LastSeen.IsZero())
	require.False(t, forwarded.LastSeen.IsZero())
	// Each attempt refreshes liveness, but shares every identity/capability field.
	direct.LastSeen, forwarded.LastSeen = time.Time{}, time.Time{}
	require.Equal(t, direct, forwarded)
}

type failingRegistrationProvider struct{ controller.ServiceDiscovery }

func (failingRegistrationProvider) LocalRegistrationPayload() ([]byte, error) {
	return nil, errors.New("injected payload failure")
}

func TestForwardedLocalRegistrationFailsClosed(t *testing.T) {
	for _, sd := range []controller.ServiceDiscovery{nil, struct{ controller.ServiceDiscovery }{}, failingRegistrationProvider{}} {
		command, err := forwardedLocalRegistrationCommand(sd, config.DefaultConfig())
		require.ErrorContains(t, err, "local registration payload")
		require.Empty(t, command)
	}
}
