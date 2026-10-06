package sdk

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestProducerInitializationCancellation(t *testing.T) {
	for _, stage := range []string{"wire", "auth", "create", "metadata", "partition-wire", "partition-auth", "second-partition-auth"} {
		t.Run(stage, func(t *testing.T) {
			cfg, entered, active := stalledInitializationServer(t, stage)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			before := producerSenderGoroutines()
			result := make(chan error, 1)
			go func() {
				p, err := NewProducerWithContext(ctx, cfg)
				if p != nil {
					_ = p.Close()
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("initialization did not reach " + stage)
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("initialization ignored cancellation at " + stage)
			}
			require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond, "initialization connections leaked")
			require.LessOrEqual(t, producerSenderGoroutines(), before, "failed initialization started sender workers")
		})
	}
}

func TestProducerControlRequestHonorsTimeoutAndContextDeadline(t *testing.T) {
	for _, parentDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "configured-timeout", true: "context-deadline"}[parentDeadline], func(t *testing.T) {
			cfg, _, active := stalledInitializationServer(t, "metadata")
			cfg.AckTimeoutMS = 100
			ctx := context.Background()
			if parentDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				cfg.AckTimeoutMS = 5000
			}
			p := &Producer{config: cfg, client: mustNewProducerClient(cfg)}
			_, err := p.controlRequest(ctx, cfg.BrokerAddrs[0], "METADATA topic=init-topic")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
		})
	}
}

func TestProducerInitializationFallsBackAfterMetadataTimeout(t *testing.T) {
	cfg, _, stalledActive := stalledInitializationServer(t, "metadata")
	healthy, _, healthyActive := stalledInitializationServer(t, "")
	cfg.AutoCreateTopics = false
	cfg.AckTimeoutMS = 200
	cfg.BrokerAddrs = append(cfg.BrokerAddrs, healthy.BrokerAddrs[0])
	p, err := NewProducerWithContext(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	require.Equal(t, healthy.BrokerAddrs[0], p.getPartitionLeaderAddr(0))
	require.NoError(t, p.Close())
	require.Eventually(t, func() bool { return stalledActive.Load() == 0 && healthyActive.Load() == 0 }, time.Second, time.Millisecond)
}

func TestProducerInitializationContextOwnsSuccessfulProducer(t *testing.T) {
	cfg, _, active := stalledInitializationServer(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := NewProducerWithContext(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	require.NotNil(t, p.client.GetConn(0))
	cancel()
	select {
	case <-p.closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("successful producer did not close with its context")
	}
	require.NoError(t, p.Close())
	require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
}

// The server completes every preceding initialization phase, then stops
// responding at the requested phase until the client closes its socket.
func stalledInitializationServer(t *testing.T, stage string) (*PublisherConfig, <-chan struct{}, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := NewDefaultPublisherConfig()
	cfg.Topic = "init-topic"
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.AutoCreateTopics = true
	cfg.Principal, cfg.AuthToken = "test-client", "synthetic-token"
	if stage == "second-partition-auth" {
		cfg.Partitions = 2
	}
	entered := make(chan struct{}, 1)
	active := &atomic.Int32{}
	var connections sync.Map
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(acceptDone)
		for ordinal := 1; ; ordinal++ {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(raw, true)
			active.Add(1)
			workers.Add(1)
			go func(ordinal int) {
				defer workers.Done()
				defer active.Add(-1)
				defer connections.Delete(raw)
				defer func() { _ = raw.Close() }()
				_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
				block := func() {
					select {
					case entered <- struct{}{}:
					default:
					}
					_, _ = io.Copy(io.Discard, raw)
				}
				if stage == "wire" || stage == "partition-wire" && ordinal == 3 {
					block()
					return
				}
				conn, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
				if err != nil {
					return
				}
				for {
					request, err := conn.ReadFrame()
					if err != nil {
						return
					}
					if request.Command == wire.CommandAuth {
						if stage == "auth" || stage == "partition-auth" && ordinal == 3 || stage == "second-partition-auth" && ordinal == 4 {
							block()
							return
						}
						if writeWireTestResponse(conn, request, "OK authenticated=true") != nil {
							return
						}
						continue
					}
					command, err := decodeWireTestCommand(request)
					if err != nil {
						return
					}
					if stage == "create" && strings.HasPrefix(command, "CREATE ") || stage == "metadata" && strings.HasPrefix(command, "METADATA ") {
						block()
						return
					}
					response := "OK topic=init-topic"
					if strings.HasPrefix(command, "METADATA ") {
						response = "OK leaders=" + strings.TrimSuffix(strings.Repeat(listener.Addr().String()+",", cfg.Partitions), ",")
					}
					if writeWireTestResponse(conn, request, response) != nil {
						return
					}
				}
			}(ordinal)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		connections.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		workers.Wait()
	})
	return cfg, entered, active
}

func producerInitializationCertificate(t *testing.T) (tls.Certificate, *x509.CertPool, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certPEM))
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
	return certificate, roots, certPath, keyPath
}

func TestProducerTLSAutocreateDoesNotSendPlaintextCredentials(t *testing.T) {
	_, _, certPath, keyPath := producerInitializationCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	observed := make(chan bool, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			observed <- false
			return
		}
		defer func() { _ = raw.Close() }()
		_ = raw.SetDeadline(time.Now().Add(time.Second))
		conn, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
		if err != nil {
			observed <- false
			return
		}
		request, err := conn.ReadFrame()
		if err != nil {
			observed <- false
			return
		}
		observed <- request.Command == wire.CommandAuth
		_ = writeWireTestResponse(conn, request, "OK authenticated=true")
		request, err = conn.ReadFrame()
		if err == nil {
			_ = writeWireTestResponse(conn, request, "ERROR: test_stop class=validation retryable=false")
		}
	}()
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.UseTLS, cfg.AutoCreateTopics = true, true
	cfg.TLSCertPath, cfg.TLSKeyPath = certPath, keyPath
	cfg.Principal, cfg.AuthToken = "test-client", "synthetic-token"
	p, err := NewProducerWithContext(context.Background(), cfg)
	require.Error(t, err)
	require.Nil(t, p)
	require.False(t, <-observed, "TLS-enabled initialization exposed AUTH to a plaintext endpoint")
}

func TestProducerControlRequestsVerifyTLS(t *testing.T) {
	for _, scenario := range []string{"address-hostname", "explicit-hostname", "wrong-hostname", "untrusted-certificate"} {
		t.Run(scenario, func(t *testing.T) { verifyProducerControlTLS(t, scenario) })
	}
}

func verifyProducerControlTLS(t *testing.T, scenario string) {
	certificate, roots, certPath, keyPath := producerInitializationCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	serverDone := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = raw.Close() }()
		_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
		conn, request, _, err := acceptWireTestRequest(raw)
		if err == nil {
			err = writeWireTestResponse(conn, request, "OK authenticated=true")
		}
		if err == nil {
			request, err = conn.ReadFrame()
		}
		if err == nil {
			err = writeWireTestResponse(conn, request, "OK topic=init-topic")
		}
		serverDone <- err
	}()
	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.UseTLS = true
	cfg.TLSCertPath, cfg.TLSKeyPath = certPath, keyPath
	cfg.Principal, cfg.AuthToken = "test-client", "synthetic-token"
	client, err := NewProducerClient(cfg)
	require.NoError(t, err)
	// Trust only this test server; hostname verification remains enabled.
	client.tlsConfig.RootCAs = roots
	switch scenario {
	case "explicit-hostname":
		client.tlsConfig.ServerName = "127.0.0.1"
	case "wrong-hostname":
		client.tlsConfig.ServerName = "wrong.example"
	case "untrusted-certificate":
		client.tlsConfig.RootCAs = x509.NewCertPool()
	}
	originalName := client.tlsConfig.ServerName
	p := &Producer{config: cfg, client: client}
	err = p.CreateTopic("init-topic", 1)
	if scenario == "wrong-hostname" || scenario == "untrusted-certificate" {
		require.ErrorContains(t, err, "certificate")
		require.Error(t, <-serverDone)
	} else {
		require.NoError(t, err)
		require.NoError(t, <-serverDone)
	}
	require.Equal(t, originalName, client.tlsConfig.ServerName, "dialing must not mutate shared TLS settings")
}
