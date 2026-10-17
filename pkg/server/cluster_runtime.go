package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/util"
)

// startServerWorker returns an idempotent cancel-and-join closure. Callers stop
// workers before closing the handlers or Raft state those workers may access.
func startServerWorker(parent context.Context, work func(context.Context)) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		work(ctx)
	}()
	return func() { cancel(); <-done }
}

func localRegistrationCurrent(sd clusterController.ServiceDiscovery, state *fsm.BrokerFSM) bool {
	provider, ok := sd.(clusterController.LocalRegistrationProvider)
	if !ok || state == nil {
		return false
	}
	payload, err := provider.LocalRegistrationPayload()
	var wanted fsm.BrokerInfo
	if err != nil || json.Unmarshal(payload, &wanted) != nil {
		return false
	}
	actual := state.GetBroker(wanted.ID)
	return actual != nil && actual.Status == "active" && actual.IncarnationID == wanted.IncarnationID &&
		actual.Addr == wanted.Addr && actual.ClientAddr == wanted.ClientAddr &&
		actual.RecoveryProtocol == wanted.RecoveryProtocol && actual.LifecycleProtocol == wanted.LifecycleProtocol &&
		actual.TransactionCoordinatorShards == wanted.TransactionCoordinatorShards
}

// Success is observed in the local FSM. A successful older registration must
// not terminate the worker before a later capability or address update arrives.
func runLocalRegistration(ctx context.Context, sd clusterController.ServiceDiscovery, cc *clusterController.ClusterController, cfg *config.Config) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if !localRegistrationCurrent(sd, cc.RaftManager.GetFSM()) {
			if err := registerLocalProcess(ctx, sd, cc, cfg); err != nil && ctx.Err() == nil {
				util.Debug("local process registration pending: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func registerLocalProcess(ctx context.Context, sd clusterController.ServiceDiscovery, cc *clusterController.ClusterController, cfg *config.Config) error {
	if err := sd.Register(); err == nil {
		return nil
	}
	command, err := forwardedLocalRegistrationCommand(sd, cfg)
	if err != nil {
		return err
	}
	response, err := cc.ForwardCommandToBrokerContext(ctx, cc.RaftManager.GetLeaderAddress(), command)
	if err != nil {
		return err
	}
	if protocol.IsErrorResponse(response) {
		return fmt.Errorf("registration rejected: %s", response)
	}
	return nil
}
