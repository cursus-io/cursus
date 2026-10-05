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
	return NewSecureClusterServerWithTokens(sd, authToken, "", tlsConfig)
}

func NewSecureClusterServerWithTokens(sd controller.ServiceDiscovery, activeToken, nextToken string, tlsConfig *tls.Config) *ClusterServer {
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
	}
	authTokens := make([]string, 0, 2)
	if activeToken != "" {
		authTokens = append(authTokens, activeToken)
	}
	if nextToken != "" && nextToken != activeToken {
		authTokens = append(authTokens, nextToken)
	}
	return &ClusterServer{
		sd:             sd,
		authTokens:     authTokens,
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
	if len(h.authTokens) == 0 {
		return true
	}
	supplied := payload.Fields["auth_token"]
	authenticated := 0
	for _, token := range h.authTokens {
		authenticated |= subtle.ConstantTimeCompare([]byte(supplied), []byte(token))
	}
	return authenticated == 1
}
