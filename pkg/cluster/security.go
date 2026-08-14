package cluster

import (
	"crypto/subtle"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
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

func (h *ClusterServer) authenticate(payload string) (string, bool) {
	if h.authToken == "" {
		return payload, true
	}
	const prefix = "AUTH "
	if !strings.HasPrefix(payload, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(payload, prefix)
	separator := strings.IndexByte(rest, ' ')
	if separator <= 0 {
		return "", false
	}
	supplied := rest[:separator]
	command := rest[separator+1:]
	if subtle.ConstantTimeCompare([]byte(supplied), []byte(h.authToken)) != 1 {
		return "", false
	}
	return command, true
}

func clusterCommandName(payload string) string {
	if index := strings.IndexByte(payload, ' '); index >= 0 {
		return payload[:index]
	}
	return payload
}
