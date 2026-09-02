package replication

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBootstrapConfigurationRequiresExplicitPeerIdentity(t *testing.T) {
	_, err := bootstrapConfiguration("broker-1", "127.0.0.1:9001", []string{"127.0.0.2:9001"})
	require.ErrorContains(t, err, "expected id@host:port")
}

func TestBootstrapConfigurationBuildsUniqueVoters(t *testing.T) {
	configuration, err := bootstrapConfiguration("broker-1", "127.0.0.1:9001", []string{
		"broker-1@127.0.0.1:9001",
		"broker-2@127.0.0.2:9001",
		"broker-3@[::1]:9001",
	})
	require.NoError(t, err)
	require.Equal(t, []raft.Server{
		{ID: "broker-1", Address: "127.0.0.1:9001", Suffrage: raft.Voter},
		{ID: "broker-2", Address: "127.0.0.2:9001", Suffrage: raft.Voter},
		{ID: "broker-3", Address: "[::1]:9001", Suffrage: raft.Voter},
	}, configuration.Servers)
}

func TestBootstrapConfigurationRejectsIdentityCollisions(t *testing.T) {
	tests := []struct {
		name  string
		peers []string
		want  string
	}{
		{name: "duplicate id", peers: []string{"broker-2@node-2:9001", "broker-2@node-3:9001"}, want: "duplicate bootstrap broker id"},
		{name: "duplicate address", peers: []string{"broker-2@node-2:9001", "broker-3@node-2:9001"}, want: "duplicate bootstrap Raft address"},
		{name: "local id mismatch", peers: []string{"broker-1@node-2:9001"}, want: "duplicate bootstrap broker id"},
		{name: "local address mismatch", peers: []string{"broker-2@127.0.0.1:9001"}, want: "duplicate bootstrap Raft address"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := bootstrapConfiguration("broker-1", "127.0.0.1:9001", test.peers)
			require.ErrorContains(t, err, test.want)
		})
	}
}
