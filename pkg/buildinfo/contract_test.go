package buildinfo_test

import (
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/buildinfo"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/require"
)

func TestBrokerProtocolMatchesRuntimeLifecycleProtocol(t *testing.T) {
	require.Equal(t, strconv.Itoa(fsm.BrokerProtocolVersionCurrent), buildinfo.BrokerProtocolVersion)
}
