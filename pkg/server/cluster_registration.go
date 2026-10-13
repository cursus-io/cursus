package server

import (
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

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
