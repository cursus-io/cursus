package config

import (
	"crypto/tls"
	"encoding/json"
)

const redactedConfigValue = "[REDACTED]"

// MarshalRedactedJSON returns diagnostic configuration JSON without live
// credentials or parsed TLS key material. It never mutates the active config.
func MarshalRedactedJSON(cfg *Config) ([]byte, error) {
	if cfg == nil {
		return json.MarshalIndent(nil, "", "  ")
	}

	safe := *cfg
	if safe.InternalAuthToken != "" {
		safe.InternalAuthToken = redactedConfigValue
	}
	safe.SASLUsers = append([]SASLUser(nil), cfg.SASLUsers...)
	for i := range safe.SASLUsers {
		if safe.SASLUsers[i].Token != "" {
			safe.SASLUsers[i].Token = redactedConfigValue
		}
	}

	// tls.Certificate contains the parsed private key behind an interface and
	// must never be handed to a generic JSON encoder.
	safe.TLSCert = tls.Certificate{}
	safe.InternalTLSCert = tls.Certificate{}
	safe.InternalTLSClientCAPool = nil
	safe.InternalTLSRootCAPool = nil

	return json.MarshalIndent(&safe, "", "  ")
}
