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
	"github.com/cursus-io/cursus/pkg/transaction"
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

func awaitRecoveryRuntime(ctx context.Context, ready <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ready:
		return ctx.Err() == nil
	}
}

// localRegistrationCurrent compares the installed process with its latest
// capability/readiness report, including readiness revocation during restore.
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
	shards := wanted.TransactionCoordinatorShards
	if shards == 0 {
		shards = transaction.DefaultCoordinatorShardCount
	}
	return actual != nil && actual.Status == "active" && actual.IncarnationID == wanted.IncarnationID &&
		actual.Addr == wanted.Addr && actual.ClientAddr == wanted.ClientAddr &&
		actual.RecoveryProtocol == wanted.RecoveryProtocol && actual.LifecycleProtocol == wanted.LifecycleProtocol &&
		actual.ConsumerRoutingReady == wanted.ConsumerRoutingReady &&
		state.TransactionCoordinatorShardCount() == shards
}

// Success is observed in the local FSM. A successful older registration must
// not terminate the worker before a later capability or address update arrives.
func runLocalRegistration(ctx context.Context, sd clusterController.ServiceDiscovery, cc *clusterController.ClusterController, cfg *config.Config) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var failures registrationFailureLog
	for {
		if ctx.Err() != nil {
			return
		}
		if localRegistrationCurrent(sd, cc.RaftManager.GetFSM()) {
			failures.recovered(time.Now())
		} else {
			if err := registerLocalProcess(ctx, sd, cc, cfg); err != nil && ctx.Err() == nil {
				failures.failed(time.Now(), err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// registerLocalProcess tries local Raft application and then leader forwarding,
// returning contextual errors only after both registration paths have failed.
func registerLocalProcess(ctx context.Context, sd clusterController.ServiceDiscovery, cc *clusterController.ClusterController, cfg *config.Config) error {
	localErr := sd.Register()
	if localErr == nil {
		return nil
	}
	leader := cc.RaftManager.GetLeaderAddress()
	failed := func(err error) error {
		return &localRegistrationError{local: localErr, final: err, leader: leader}
	}
	command, err := forwardedLocalRegistrationCommand(sd, cfg)
	if err != nil {
		return failed(err)
	}
	if leader == "" {
		return failed(fmt.Errorf("raft leader unavailable"))
	}
	response, err := cc.ForwardCommandToBrokerContext(ctx, leader, command)
	if err != nil {
		return failed(err)
	}
	if protocol.IsErrorResponse(response) {
		return failed(fmt.Errorf("registration rejected: %s", response))
	}
	return nil
}
