package replication

import (
	"fmt"
	"net"
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

	configuration, err := bootstrapConfiguration(rm.brokerID, rm.localAddr, peers)
	if err != nil {
		return err
	}

	future := rm.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	util.Info("completed with %d servers", len(configuration.Servers))
	return nil
}

func bootstrapConfiguration(brokerID, localAddr string, peers []string) (raft.Configuration, error) {
	if strings.TrimSpace(brokerID) == "" {
		return raft.Configuration{}, fmt.Errorf("bootstrap broker id is required")
	}
	if err := validateRaftAddress(localAddr); err != nil {
		return raft.Configuration{}, fmt.Errorf("invalid local Raft address %q: %w", localAddr, err)
	}

	configuration := raft.Configuration{Servers: []raft.Server{{
		ID: raft.ServerID(brokerID), Address: raft.ServerAddress(localAddr), Suffrage: raft.Voter,
	}}}
	ids := map[string]string{brokerID: localAddr}
	addresses := map[string]string{localAddr: brokerID}
	for _, rawPeer := range peers {
		peer := strings.TrimSpace(rawPeer)
		if peer == "" {
			return raft.Configuration{}, fmt.Errorf("bootstrap peer entry is empty")
		}
		if strings.Count(peer, "@") != 1 {
			return raft.Configuration{}, fmt.Errorf("invalid bootstrap peer %q: expected id@host:port", rawPeer)
		}
		parts := strings.SplitN(peer, "@", 2)
		peerID := strings.TrimSpace(parts[0])
		peerAddr := strings.TrimSpace(parts[1])
		if peerID == "" {
			return raft.Configuration{}, fmt.Errorf("invalid bootstrap peer %q: broker id is required", rawPeer)
		}
		if err := validateRaftAddress(peerAddr); err != nil {
			return raft.Configuration{}, fmt.Errorf("invalid bootstrap peer %q: %w", rawPeer, err)
		}
		if peerID == brokerID && peerAddr == localAddr {
			continue
		}
		if existingAddr, duplicate := ids[peerID]; duplicate {
			return raft.Configuration{}, fmt.Errorf("duplicate bootstrap broker id %q uses %q and %q", peerID, existingAddr, peerAddr)
		}
		if existingID, duplicate := addresses[peerAddr]; duplicate {
			return raft.Configuration{}, fmt.Errorf("duplicate bootstrap Raft address %q belongs to %q and %q", peerAddr, existingID, peerID)
		}
		ids[peerID] = peerAddr
		addresses[peerAddr] = peerID
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID: raft.ServerID(peerID), Address: raft.ServerAddress(peerAddr), Suffrage: raft.Voter,
		})
		util.Debug("bootstrap add peer: id=%s addr=%s", peerID, peerAddr)
	}
	return configuration, nil
}

func validateRaftAddress(address string) error {
	if strings.TrimSpace(address) != address || address == "" {
		return fmt.Errorf("address must be a non-empty host:port without surrounding whitespace")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("expected host:port: %w", err)
	}
	if port == "" {
		return fmt.Errorf("port is required")
	}
	return nil
}
