package replication

import (
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

func (rm *RaftReplicationManager) BootstrapCluster(peers []string) error {
	if confFut := rm.raft.GetConfiguration(); confFut.Error() == nil {
		if len(confFut.Configuration().Servers) > 0 {
			util.Info("bootstrap skipped: existing configuration present with %d servers", len(confFut.Configuration().Servers))
			return nil
		}
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       raft.ServerID(rm.brokerID),
				Address:  raft.ServerAddress(rm.localAddr),
				Suffrage: raft.Voter,
			},
		},
	}

	for _, peer := range peers {
		if peer == rm.localAddr || peer == fmt.Sprintf("%s@%s", rm.brokerID, rm.localAddr) {
			continue // skip self
		}

		var peerID, peerAddr string
		if strings.Contains(peer, "@") {
			parts := strings.SplitN(peer, "@", 2)
			peerID = parts[0]
			peerAddr = parts[1]
		} else if strings.Contains(peer, ":") {
			util.Warn("peer entry '%s' uses legacy host:port format; consider using 'id@addr' to avoid id collisions", peer)
			peerAddr = peer
			peerID = strings.Split(peer, ":")[0]
		} else {
			util.Error("invalid peer format: %s", peer)
			continue
		}

		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:       raft.ServerID(peerID),
			Address:  raft.ServerAddress(peerAddr),
			Suffrage: raft.Voter,
		})
		util.Debug("bootstrap add peer: id=%s addr=%s", peerID, peerAddr)
	}

	future := rm.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	util.Info("completed with %d servers", len(configuration.Servers))
	return nil
}
