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

func newRaftTLSStreamLayer(bindAddress string, advertised *net.TCPAddr, serverConfig, clientConfig *tls.Config) (*raftTLSStreamLayer, error) {
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

func newRaftNetworkTransport(cfg *config.Config, bindAddress string, advertised *net.TCPAddr) (*raft.NetworkTransport, error) {
	const timeout = 10 * time.Second
	if !cfg.InternalUseTLS {
		return raft.NewTCPTransport(bindAddress, advertised, 3, timeout, os.Stderr)
	}

	layer, err := newRaftTLSStreamLayer(bindAddress, advertised, cfg.InternalServerTLSConfig(), cfg.InternalClientTLSConfig())
	if err != nil {
		return nil, err
	}
	return raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  layer,
		MaxPool: 3,
		Timeout: timeout,
	}), nil
}
