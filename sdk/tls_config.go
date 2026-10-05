package sdk

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func buildClientTLSConfig(caPath, serverName, certificatePath, keyPath string) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if caPath != "" {
		// Extend the host trust store so configuring a private deployment CA does
		// not unexpectedly remove public roots needed by other broker endpoints.
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		// #nosec G304 -- caPath is an explicit SDK trust configuration supplied by the caller.
		caPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA bundle: %w", err)
		}
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("TLS CA bundle contains no valid certificates")
		}
		config.RootCAs = roots
	}
	if certificatePath != "" {
		certificate, err := tls.LoadX509KeyPair(certificatePath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}
