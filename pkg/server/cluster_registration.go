package server

import (
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
)

// forwardedLocalRegistrationCommand uses exactly the identity and capabilities
// owned by direct registration. Missing providers fail closed rather than invent
// a second payload with potentially different capabilities or incarnation.
func forwardedLocalRegistrationCommand(sd controller.ServiceDiscovery, cfg *config.Config) (string, error) {
	provider, ok := sd.(controller.LocalRegistrationProvider)
	if !ok {
		return "", fmt.Errorf("local registration payload unavailable")
	}
	payload, err := provider.LocalRegistrationPayload()
	if err != nil {
		return "", fmt.Errorf("local registration payload: %w", err)
	}
	return fmt.Sprintf("RAFT_APPLY %stype=REGISTER payload=%s", internalAuthPrefix(cfg), payload), nil
}

func brokerRegistrationReadinessError(state *fsm.BrokerFSM, brokerID, incarnationID string) error {
	if state == nil {
		return fmt.Errorf("cluster registration state unavailable")
	}
	broker := state.GetBroker(brokerID)
	if broker == nil || !strings.EqualFold(broker.Status, "active") {
		return fmt.Errorf("local broker registration is not active")
	}
	if incarnationID == "" || broker.IncarnationID != incarnationID {
		return fmt.Errorf("local broker registration belongs to a previous process incarnation")
	}
	return nil
}
