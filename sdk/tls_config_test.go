package sdk

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

type sdkTLSFixture struct {
	caPath         string
	clientCertPath string
	clientKeyPath  string
	serverCert     tls.Certificate
	clientCAPool   *x509.CertPool
}

func TestSDKClientsTrustPrivateCAWithoutClientIdentity(t *testing.T) {
	fixture := newSDKTLSFixture(t)
	for _, clientKind := range []string{"producer", "consumer", "admin"} {
		t.Run(clientKind, func(t *testing.T) {
			addr, serverResult := startSDKTLSServer(t, fixture, false, clientKind == "admin")
			err := connectSDKTLSTestClient(t, clientKind, addr, fixture, "broker.internal", false)
			require.NoError(t, err)
			require.NoError(t, receiveSDKTLSServerResult(t, serverResult))
		})
	}
}

func TestSDKClientsRejectHostnameMismatch(t *testing.T) {
	fixture := newSDKTLSFixture(t)
	for _, clientKind := range []string{"producer", "consumer", "admin"} {
		t.Run(clientKind, func(t *testing.T) {
			addr, serverResult := startSDKTLSServer(t, fixture, false, clientKind == "admin")
			err := connectSDKTLSTestClient(t, clientKind, addr, fixture, "other.internal", false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "certificate")
			_ = receiveSDKTLSServerResult(t, serverResult)
		})
	}
}

func TestSDKClientsSupportRequiredMutualTLS(t *testing.T) {
	fixture := newSDKTLSFixture(t)
	for _, clientKind := range []string{"producer", "consumer", "admin"} {
		t.Run(clientKind+"/without_identity", func(t *testing.T) {
			addr, serverResult := startSDKTLSServer(t, fixture, true, clientKind == "admin")
			err := connectSDKTLSTestClient(t, clientKind, addr, fixture, "broker.internal", false)
			require.Error(t, err)
			_ = receiveSDKTLSServerResult(t, serverResult)
		})
		t.Run(clientKind+"/with_identity", func(t *testing.T) {
			addr, serverResult := startSDKTLSServer(t, fixture, true, clientKind == "admin")
			err := connectSDKTLSTestClient(t, clientKind, addr, fixture, "broker.internal", true)
			require.NoError(t, err)
			require.NoError(t, receiveSDKTLSServerResult(t, serverResult))
		})
	}
}

func TestSDKClientTLSConfigurationContract(t *testing.T) {
	config, err := buildClientTLSConfig("", "", "", "")
	require.NoError(t, err)
	require.Nil(t, config.RootCAs, "nil RootCAs delegates to the platform trust store")
	require.Empty(t, config.Certificates)
	require.False(t, config.InsecureSkipVerify)

	for _, paths := range [][2]string{{"client.crt", ""}, {"", "client.key"}} {
		err := validateTLSFiles(true, "", "", paths[0], paths[1])
		require.ErrorContains(t, err, "configured together")
	}
	require.Error(t, validateTLSFiles(true, "", "bad name", "", ""))

	invalidCA := filepath.Join(t.TempDir(), "invalid-ca.pem")
	require.NoError(t, os.WriteFile(invalidCA, []byte("not a certificate"), 0o600))
	_, err = buildClientTLSConfig(invalidCA, "broker.internal", "", "")
	require.ErrorContains(t, err, "contains no valid certificates")
}

func connectSDKTLSTestClient(t *testing.T, clientKind, addr string, fixture sdkTLSFixture, serverName string, withIdentity bool) error {
	t.Helper()
	certPath, keyPath := "", ""
	if withIdentity {
		certPath, keyPath = fixture.clientCertPath, fixture.clientKeyPath
	}
	switch clientKind {
	case "producer":
		config := NewDefaultPublisherConfig()
		config.BrokerAddrs = []string{addr}
		config.UseTLS = true
		config.TLSCAPath = fixture.caPath
		config.TLSServerName = serverName
		config.TLSCertPath = certPath
		config.TLSKeyPath = keyPath
		client, err := NewProducerClient(config)
		if err != nil {
			return err
		}
		defer func() { _ = client.Close() }()
		return client.ConnectPartition(0, addr)
	case "consumer":
		config := NewDefaultConsumerConfig()
		config.BrokerAddrs = []string{addr}
		config.UseTLS = true
		config.TLSCAPath = fixture.caPath
		config.TLSServerName = serverName
		config.TLSCertPath = certPath
		config.TLSKeyPath = keyPath
		client, err := NewConsumerClient(config)
		if err != nil {
			return err
		}
		conn, err := client.Connect(addr)
		if conn != nil {
			_ = conn.Close()
		}
		return err
	case "admin":
		client, err := NewAdminClient(&AdminConfig{
			BrokerAddrs: []string{addr}, UseTLS: true,
			TLSCAPath: fixture.caPath, TLSServerName: serverName,
			TLSCertPath: certPath, TLSKeyPath: keyPath,
			RequestTimeoutMS: 1000, HandshakeTimeoutMS: 1000,
		})
		if err != nil {
			return err
		}
		_, err = client.executeOnce(context.Background(), addr, "LIST")
		return err
	default:
		return errors.New("unknown SDK client kind")
	}
}

func startSDKTLSServer(t *testing.T, fixture sdkTLSFixture, requireClient, respond bool) (string, <-chan error) {
	t.Helper()
	serverConfig := &tls.Config{Certificates: []tls.Certificate{fixture.serverCert}, MinVersion: tls.VersionTLS12}
	if requireClient {
		serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
		serverConfig.ClientCAs = fixture.clientCAPool
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		defer func() { _ = listener.Close() }()
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		defer func() { _ = conn.Close() }()
		if respond {
			connection, request, command, err := acceptWireTestRequest(conn)
			if err == nil && command != "LIST" {
				err = errors.New("unexpected admin command: " + command)
			}
			if err == nil {
				err = writeWireTestResponse(connection, request, "OK")
			}
			result <- err
			return
		}
		_, err = wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		result <- err
	}()
	return listener.Addr().String(), result
}

func receiveSDKTLSServerResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("TLS test server did not finish")
		return nil
	}
}

func newSDKTLSFixture(t *testing.T) sdkTLSFixture {
	t.Helper()
	now := time.Now()
	caCert, caKey, caPEM, _ := makeSDKTestCertificate(t, x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Cursus SDK test CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}, nil, nil)
	_, _, serverCertPEM, serverKeyPEM := makeSDKTestCertificate(t, x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "broker.internal"},
		DNSNames: []string{"broker.internal"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, caCert, caKey)
	_, _, clientCertPEM, clientKeyPEM := makeSDKTestCertificate(t, x509.Certificate{
		SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "sdk-client"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, caKey)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)
	dir := t.TempDir()
	fixture := sdkTLSFixture{
		caPath: filepath.Join(dir, "ca.crt"), clientCertPath: filepath.Join(dir, "client.crt"),
		clientKeyPath: filepath.Join(dir, "client.key"), serverCert: serverCert, clientCAPool: x509.NewCertPool(),
	}
	require.True(t, fixture.clientCAPool.AppendCertsFromPEM(caPEM))
	require.NoError(t, os.WriteFile(fixture.caPath, caPEM, 0o600))
	require.NoError(t, os.WriteFile(fixture.clientCertPath, clientCertPEM, 0o600))
	require.NoError(t, os.WriteFile(fixture.clientKeyPath, clientKeyPEM, 0o600))
	return fixture
}

func makeSDKTestCertificate(t *testing.T, template x509.Certificate, parent *x509.Certificate, parentKey ed25519.PrivateKey) (*x509.Certificate, ed25519.PrivateKey, []byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	if parent == nil {
		parent, parentKey = &template, privateKey
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, parent, publicKey, parentKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	return certificate, privateKey,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestDerivedTLSServerNameUsesDialHost(t *testing.T) {
	fixture := newSDKTLSFixture(t)
	config, err := buildClientTLSConfig(fixture.caPath, "", "", "")
	require.NoError(t, err)
	addr, result := startSDKTLSServer(t, fixture, false, false)
	_, err = dialAuthenticatedWireConnection(context.Background(), addr, time.Second, 1000, "none", config, "", "")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "IP SAN") || strings.Contains(err.Error(), "certificate"), err)
	_ = receiveSDKTLSServerResult(t, result)
}
