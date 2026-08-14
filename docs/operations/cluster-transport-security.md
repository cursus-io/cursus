# Cluster transport security

Distributed mode now fails configuration validation unless both of these are
configured:

- `internal_auth_token` for discovery and internal commands.
- `internal_use_tls: true` with a broker certificate, private key, trusted CA,
  and client server name for mutual TLS on discovery, the internal broker
  listener, and Raft transport.

`allow_insecure_cluster_transport: true` is an explicit escape hatch for
isolated test fixtures only. It must not be used for production.

## Upgrade constraint

TLS Raft framing is not wire-compatible with the previous plaintext Raft
transport. Do not perform a mixed plaintext/TLS rolling upgrade. Validate all
certificates and advertised DNS names first, stop the cluster cleanly, update
all members to the secured build and configuration, then start the complete
quorum. Verify discovery membership, Raft leadership, in-sync replicas, group
registration, committed offsets, and readiness before restoring client traffic.

Each broker certificate must be signed by the configured internal CA and valid
for the advertised hostname used by peers. The configured
`internal_tls_server_name` must match the certificate SAN. A discovery bind
failure, TLS validation failure, Raft initialization error, or missing security
configuration prevents readiness instead of falling back to plaintext.
