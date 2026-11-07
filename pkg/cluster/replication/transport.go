package replication

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/hashicorp/raft"
)

type raftTLSStreamLayer struct {
	listener     net.Listener
	advertised   net.Addr
	clientConfig *tls.Config
}

// stableRaftAddress preserves a configured DNS name instead of resolving it to
// a Pod IP. StatefulSet Pods may be recreated with a different IP while the
// Raft membership address remains their stable headless-service DNS name.
type stableRaftAddress string

func (a stableRaftAddress) Network() string { return "tcp" }
func (a stableRaftAddress) String() string  { return string(a) }

func newRaftTLSStreamLayer(bindAddress string, advertised net.Addr, serverConfig, clientConfig *tls.Config) (*raftTLSStreamLayer, error) {
	if serverConfig == nil || clientConfig == nil {
		return nil, fmt.Errorf("raft TLS requires server and client TLS configuration")
	}
	raw, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}
	return &raftTLSStreamLayer{
		listener:     tls.NewListener(raw, serverConfig.Clone()),
		advertised:   advertised,
		clientConfig: clientConfig.Clone(),
	}, nil
}

func (l *raftTLSStreamLayer) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *raftTLSStreamLayer) Close() error {
	return l.listener.Close()
}

func (l *raftTLSStreamLayer) Addr() net.Addr {
	return l.advertised
}

func (l *raftTLSStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), l.clientConfig.Clone())
}

// newRaftNetworkTransport constructs the configured TCP or TLS transport with voter-identity validation.
func newRaftNetworkTransport(cfg *config.Config, bindAddress, advertised string) (*identityRaftTransport, error) {
	const timeout = 10 * time.Second
	if !cfg.InternalUseTLS {
		resolved, err := net.ResolveTCPAddr("tcp", advertised)
		if err != nil {
			return nil, fmt.Errorf("resolve advertised Raft address %q: %w", advertised, err)
		}
		transport, err := raft.NewTCPTransport(bindAddress, resolved, 3, timeout, os.Stderr)
		if err != nil {
			return nil, err
		}
		return &identityRaftTransport{transport}, nil
	}

	layer, err := newRaftTLSStreamLayer(bindAddress, stableRaftAddress(advertised), cfg.InternalServerTLSConfig(), cfg.InternalClientTLSConfig())
	if err != nil {
		return nil, err
	}
	return &identityRaftTransport{raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  layer,
		MaxPool: 3,
		Timeout: timeout,
	})}, nil
}
