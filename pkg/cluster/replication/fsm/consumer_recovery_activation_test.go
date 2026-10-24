package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestRecoveryCheckedRoutingActivationRejectsStaleProcessSet(t *testing.T) {
	for _, mutation := range []string{"none", "not ready", "inactive", "old binary", "restart", "joined", "missing", "duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			f := NewBrokerFSM(nil, nil)
			registerConsumerRoutingBroker(t, f, "a", "active", 2)
			registerConsumerRoutingBroker(t, f, "b", "active", 2)
			command := ConsumerRoutingActivation{Voters: []string{"a", "b"}}
			for _, id := range []string{"a", "b"} {
				broker := f.brokers[id]
				broker.LifecycleProtocol = BrokerProtocolVersionCurrent
				broker.ConsumerRoutingReady = true
				command.Processes = append(command.Processes, BootstrapProcessIdentity{BrokerID: id, IncarnationID: broker.IncarnationID, IncarnationEpoch: broker.IncarnationEpoch})
			}
			switch mutation {
			case "not ready":
				f.brokers["b"].ConsumerRoutingReady = false
			case "inactive":
				f.brokers["b"].Status = "inactive"
			case "old binary":
				// An older process does not publish the additive readiness field.
				f.brokers["b"].ConsumerRoutingReady = false
			case "restart":
				f.brokers["b"].IncarnationEpoch++
			case "joined":
				registerConsumerRoutingBroker(t, f, "late", "active", 2)
			case "missing":
				command.Processes = command.Processes[:1]
			case "duplicate":
				command.Processes[1] = command.Processes[0]
			}
			data, err := json.Marshal(command)
			require.NoError(t, err)
			result := f.Apply(&raft.Log{Data: append([]byte("CONSUMER_ROUTING_ACTIVATE:"), data...)})
			if mutation == "none" {
				require.Nil(t, result)
				require.True(t, f.ConsumerMetadataRoutingEnabled())
			} else {
				require.Error(t, resultError(result))
				require.False(t, f.ConsumerMetadataRoutingEnabled())
			}
		})
	}
}
