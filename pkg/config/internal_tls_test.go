package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInternalBrokerMTLSConfigWithFixtureCertificates(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, caKey, caCert := mustCreateTestCA(t)
	brokerCertPEM, brokerKeyPEM := mustCreateBrokerCert(t, caCert, caKey)

	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "broker.pem")
	keyPath := filepath.Join(dir, "broker-key.pem")
	if err := os.WriteFile(caPath, caCertPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(certPath, brokerCertPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, brokerKeyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cfg := DefaultConfig()
	cfg.InternalUseTLS = true
	cfg.InternalBrokerPort = 19000
	cfg.InternalTLSCAPath = caPath
	cfg.InternalTLSCertPath = certPath
	cfg.InternalTLSKeyPath = keyPath
	cfg.InternalTLSServerName = "localhost"
	if err := cfg.loadInternalTLSConfig(); err != nil {
		t.Fatalf("loadInternalTLSConfig failed: %v", err)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg.InternalServerTLSConfig())
	if err != nil {
		t.Fatalf("tls listen failed: %v", err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_, err = conn.Write([]byte("ok"))
		serverErr <- err
	}()

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), cfg.InternalClientTLSConfig())
	if err != nil {
		t.Fatalf("tls dial failed: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, 2)
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("tls read failed: %v", err)
	}
	if string(buf) != "ok" {
		t.Fatalf("unexpected payload %q", string(buf))
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server write failed: %v", err)
	}
}

func mustCreateTestCA(t *testing.T) ([]byte, *rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cursus-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, cert
}

func mustCreateBrokerCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate broker key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create broker cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}
