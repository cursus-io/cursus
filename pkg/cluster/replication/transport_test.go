package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftAdvertiseAddrPreservesDNSHost(t *testing.T) {
	addr, err := newRaftAdvertiseAddr("cursus-0.cursus-headless.default.svc.cluster.local:9001")
	require.NoError(t, err)
	require.Equal(t, "cursus-0.cursus-headless.default.svc.cluster.local:9001", addr.String())
}

func TestRaftAdvertiseAddrRejectsInvalidAddress(t *testing.T) {
	for _, address := range []string{"", "host", ":9001", "host:0", "host:not-a-port"} {
		_, err := newRaftAdvertiseAddr(address)
		require.Error(t, err, address)
	}
}
