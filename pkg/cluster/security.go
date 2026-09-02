package cluster

import (
	"crypto/subtle"
	"crypto/tls"
	"net"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/wire"
)

const (
	defaultClusterMaxConnections = 128
	defaultClusterRequestTimeout = 10 * time.Second
)

func NewSecureClusterServer(sd controller.ServiceDiscovery, authToken string, tlsConfig *tls.Config) *ClusterServer {
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
	}
	return &ClusterServer{
		sd:             sd,
		authToken:      authToken,
		tlsConfig:      tlsConfig,
		connectionSlot: make(chan struct{}, defaultClusterMaxConnections),
		requestTimeout: defaultClusterRequestTimeout,
	}
}

func listenCluster(address string, tlsConfig *tls.Config) (net.Listener, error) {
	raw, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		return raw, nil
	}
	return tls.NewListener(raw, tlsConfig.Clone()), nil
}

func (h *ClusterServer) authenticate(payload wire.CommandPayload) bool {
	if h.authToken == "" {
		return true
	}
	supplied := payload.Fields["auth_token"]
	return subtle.ConstantTimeCompare([]byte(supplied), []byte(h.authToken)) == 1
}
