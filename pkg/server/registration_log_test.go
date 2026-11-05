package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// lockedRegistrationLogs permits assertions while the registration worker runs.
type lockedRegistrationLogs struct {
	sync.Mutex
	bytes.Buffer
}

func (b *lockedRegistrationLogs) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedRegistrationLogs) text() string {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.String()
}

func captureRegistrationLogs(t *testing.T) *lockedRegistrationLogs {
	t.Helper()
	b := &lockedRegistrationLogs{}
	previous := log.Writer()
	log.SetOutput(b)
	t.Cleanup(func() { log.SetOutput(previous) })
	return b
}

func TestRegistrationFailureLogsFirstChangeRepeatAndRecovery(t *testing.T) {
	logs := captureRegistrationLogs(t)
	var state registrationFailureLog
	now := time.Now()
	cert := &x509.Certificate{Raw: []byte("synthetic"), NotAfter: now.Add(-time.Hour)}
	failure := func(detail string) error {
		return &localRegistrationError{leader: "leader:9001", local: raft.ErrNotLeader,
			final: fmt.Errorf("TLS: %w", x509.CertificateInvalidError{Cert: cert, Reason: x509.Expired, Detail: detail})}
	}
	state.failed(now, failure("current time A is after expiry"))
	state.failed(now.Add(time.Second), failure("current time B is after expiry"))
	require.Equal(t, 1, strings.Count(logs.text(), "[WARN]"))
	state.failed(now.Add(time.Minute), failure("current time C is after expiry"))
	require.Contains(t, logs.text(), "suppressed=1")
	state.failed(now.Add(time.Minute+time.Second), errors.New("connection refused"))
	require.Equal(t, 3, strings.Count(logs.text(), "[WARN]"))
	state.recovered(now.Add(2 * time.Minute))
	state.recovered(now.Add(3 * time.Minute))
	require.Equal(t, 1, strings.Count(logs.text(), "registration recovered:"))
	state.failed(now.Add(4*time.Minute), failure("current time D"))
	require.Equal(t, 4, strings.Count(logs.text(), "[WARN]"))
	require.Contains(t, logs.text(), "certificate has expired")
	require.Contains(t, logs.text(), "local registration: node is not the leader")
}

func TestRegistrationFailureKeysIgnoreLocalPortButDetectLeaderAndCertificate(t *testing.T) {
	network := func(port int) error {
		return &net.OpError{Op: "read", Net: "tcp", Source: &net.TCPAddr{Port: port}, Addr: &net.TCPAddr{Port: 9000}, Err: errors.New("connection reset")}
	}
	require.Equal(t, registrationFailureKey(network(1)), registrationFailureKey(network(2)))
	a := &localRegistrationError{leader: "a", local: raft.ErrNotLeader, final: network(1)}
	b := &localRegistrationError{leader: "b", local: raft.ErrNotLeader, final: network(1)}
	require.NotEqual(t, registrationFailureKey(a), registrationFailureKey(b))
	expired := func(raw string) error {
		return x509.CertificateInvalidError{Cert: &x509.Certificate{Raw: []byte(raw)}, Reason: x509.Expired}
	}
	require.NotEqual(t, registrationFailureKey(expired("a")), registrationFailureKey(expired("b")))
}

// forwardingRegistrationRaft keeps leader changes independent of the local FSM.
type forwardingRegistrationRaft struct {
	clusterController.RaftManager
	state  *fsm.BrokerFSM
	mu     sync.Mutex
	leader string
}

func (m *forwardingRegistrationRaft) GetFSM() *fsm.BrokerFSM            { return m.state }
func (m *forwardingRegistrationRaft) ApplyCommand(string, []byte) error { return raft.ErrNotLeader }
func (m *forwardingRegistrationRaft) GetLeaderAddress() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leader
}
func (m *forwardingRegistrationRaft) setLeader(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leader = addr
}

func registrationForwardingFixture(t *testing.T, listener net.Listener, cfg *config.Config) (*forwardingRegistrationRaft, clusterController.ServiceDiscovery, *clusterController.ClusterController) {
	t.Helper()
	rm := &forwardingRegistrationRaft{state: fsm.NewBrokerFSM(nil, nil), leader: "127.0.0.1:9001"}
	cfg.InternalBrokerPort = listener.Addr().(*net.TCPAddr).Port
	cfg.ClientRequestTimeoutMS = 1000
	sd := clusterController.NewServiceDiscovery(rm, "follower", "127.0.0.1:9002", "127.0.0.1:9000")
	cc := &clusterController.ClusterController{RaftManager: rm, Router: clusterController.NewClusterRouter("follower", "127.0.0.1:9002", nil, rm, 9000, "", cfg)}
	return rm, sd, cc
}

// serveRegistrationWire applies the forwarded REGISTER as if replicated back
// to the follower; it can reject attempts without advancing that follower FSM.
func serveRegistrationWire(t *testing.T, listener net.Listener, state *fsm.BrokerFSM, reject *bool, mu *sync.Mutex) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer raw.Close()
				_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
				conn, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
				if err != nil {
					return
				}
				request, err := conn.ReadFrame()
				if err != nil {
					return
				}
				response := "ERROR: not leader"
				mu.Lock()
				denied := *reject
				mu.Unlock()
				if !denied {
					payload, err := wire.DecodeCommandPayload(request.Payload)
					if err != nil || payload.Fields["type"] != "REGISTER" {
						t.Errorf("invalid forwarded REGISTER: %v", err)
						return
					}
					if err := state.Apply(&raft.Log{Data: []byte("REGISTER:" + payload.Fields["payload"])}); err != nil {
						t.Errorf("apply forwarded REGISTER: %v", err)
						return
					}
					response = "OK"
				}
				_ = conn.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: []byte(response)})
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
}

func TestRegistrationFollowerForwardsWithoutErrorLog(t *testing.T) {
	logs := captureRegistrationLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	rm, sd, cc := registrationForwardingFixture(t, listener, cfg)
	var reject bool
	var mu sync.Mutex
	serveRegistrationWire(t, listener, rm.state, &reject, &mu)
	require.NoError(t, registerLocalProcess(context.Background(), sd, cc, cfg))
	require.True(t, localRegistrationCurrent(sd, rm.state))
	require.NotContains(t, logs.text(), "[ERROR]")
	require.NotContains(t, logs.text(), "[WARN]")
}

func TestRegistrationWorkerRetriesLeaderChangeAndInactiveRegistration(t *testing.T) {
	logs := captureRegistrationLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	rm, sd, cc := registrationForwardingFixture(t, listener, cfg)
	reject := true
	var mu sync.Mutex
	serveRegistrationWire(t, listener, rm.state, &reject, &mu)
	rm.setLeader("")
	stop := startServerWorker(context.Background(), func(ctx context.Context) { runLocalRegistration(ctx, sd, cc, cfg) })
	t.Cleanup(stop)
	require.Eventually(t, func() bool { return strings.Contains(logs.text(), "raft leader unavailable") }, time.Second, time.Millisecond)
	rm.setLeader("127.0.0.1:9003")
	require.Eventually(t, func() bool { return strings.Contains(logs.text(), "registration rejected") }, 3*time.Second, 10*time.Millisecond)
	mu.Lock()
	reject = false
	mu.Unlock()
	require.Eventually(t, func() bool { return strings.Contains(logs.text(), "registration recovered:") }, 4*time.Second, 10*time.Millisecond)
	broker := rm.state.GetBroker("follower")
	require.NotNil(t, broker)
	require.Nil(t, rm.state.Apply(&raft.Log{Data: []byte(fmt.Sprintf("DEREGISTER:{\"id\":\"follower\",\"incarnation_id\":%q,\"incarnation_epoch\":%d}", broker.IncarnationID, broker.IncarnationEpoch))}))
	require.Eventually(t, func() bool { return localRegistrationCurrent(sd, rm.state) }, 3*time.Second, 10*time.Millisecond)
	stop()
	require.NotContains(t, logs.text(), "[ERROR]")
	require.Equal(t, 1, strings.Count(logs.text(), "registration recovered:"))
}

func TestRegistrationExpiredTLSIsPreservedAndVisible(t *testing.T) {
	logs := captureRegistrationLogs(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-2 * time.Hour), NotAfter: time.Now().Add(-time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(certificate)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}})
	require.NoError(t, err)
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			_ = conn.(*tls.Conn).Handshake()
		}
	}()
	cfg := config.DefaultConfig()
	cfg.InternalUseTLS, cfg.InternalTLSRootCAPool = true, pool
	rm, sd, cc := registrationForwardingFixture(t, listener, cfg)
	err = registerLocalProcess(context.Background(), sd, cc, cfg)
	var invalid x509.CertificateInvalidError
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, x509.Expired, invalid.Reason)
	var failures registrationFailureLog
	failures.failed(time.Now(), err)
	require.Contains(t, logs.text(), "[WARN]")
	require.Contains(t, logs.text(), "certificate has expired")
	require.Contains(t, logs.text(), "leader=\"127.0.0.1:9001\"")
	require.False(t, localRegistrationCurrent(sd, rm.state))
	<-done
}
