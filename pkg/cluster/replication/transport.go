package replication

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/hashicorp/raft"
)

type raftTLSStreamLayer struct {
	listener     net.Listener
	advertised   net.Addr
	clientConfig *tls.Config
}

type raftTCPStreamLayer struct {
	listener   net.Listener
	advertised net.Addr
}

type raftAdvertiseAddr struct {
	address string
}

func (a raftAdvertiseAddr) Network() string { return "tcp" }

func (a raftAdvertiseAddr) String() string { return a.address }

func newRaftAdvertiseAddr(address string) (net.Addr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, fmt.Errorf("advertised host is required")
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return nil, fmt.Errorf("invalid advertised port %q", port)
	}
	return raftAdvertiseAddr{address: net.JoinHostPort(host, port)}, nil
}

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

func newRaftTCPStreamLayer(bindAddress string, advertised net.Addr) (*raftTCPStreamLayer, error) {
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}
	return &raftTCPStreamLayer{listener: listener, advertised: advertised}, nil
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

func (l *raftTCPStreamLayer) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *raftTCPStreamLayer) Close() error {
	return l.listener.Close()
}

func (l *raftTCPStreamLayer) Addr() net.Addr {
	return l.advertised
}

func (l *raftTCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func newRaftNetworkTransport(cfg *config.Config, bindAddress string, advertised net.Addr) (*raft.NetworkTransport, error) {
	const timeout = 10 * time.Second
	var layer raft.StreamLayer
	var err error
	if !cfg.InternalUseTLS {
		layer, err = newRaftTCPStreamLayer(bindAddress, advertised)
	} else {
		layer, err = newRaftTLSStreamLayer(bindAddress, advertised, cfg.InternalServerTLSConfig(), cfg.InternalClientTLSConfig())
	}
	if err != nil {
		return nil, err
	}
	return raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  layer,
		MaxPool: 3,
		Timeout: timeout,
	}), nil
}
