package fsm

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBrokerFSMRestoreInitializesMapInvariants(t *testing.T) {
	for _, version := range []int{0, 2} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			brokerFSM := NewBrokerFSM(nil, nil)
			snapshot := fmt.Sprintf(`{"version":%d}`, version)
			require.NoError(t, brokerFSM.Restore(io.NopCloser(strings.NewReader(snapshot))))

			brokerFSM.mu.RLock()
			require.NotNil(t, brokerFSM.logs)
			require.NotNil(t, brokerFSM.brokers)
			require.NotNil(t, brokerFSM.partitionMetadata)
			require.NotNil(t, brokerFSM.producerState)
			require.NotNil(t, brokerFSM.notifiers)
			brokerFSM.mu.RUnlock()

			var registerResult interface{}
			require.NotPanics(t, func() {
				registerResult = brokerFSM.Apply(&raft.Log{
					Index: 1,
					Data:  []byte(`REGISTER:{"id":"node-1","addr":"127.0.0.1:9000","status":"active"}`),
				})
			})
			require.Nil(t, registerResult)
			require.Equal(t, "node-1", brokerFSM.GetBroker("node-1").ID)

			var partitionResult interface{}
			require.NotPanics(t, func() {
				partitionResult = brokerFSM.Apply(&raft.Log{
					Index: 2,
					Data: []byte(`PARTITION:orders-0:{"leader":"node-1","replicas":["node-1"],` +
						`"isr":["node-1"],"leader_epoch":1,"partition_count":1}`),
				})
			})
			require.Nil(t, partitionResult)
			require.Equal(t, "node-1", brokerFSM.GetPartitionMetadata("orders-0").Leader)
		})
	}
}
