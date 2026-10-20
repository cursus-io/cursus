# Installation

## Requirements

- Go 1.25.0 or newer for source builds (`go.mod` is authoritative).
- [Docker Engine 28.3.3 or newer](https://docs.docker.com/engine/release-notes/28/#2833) with Compose for bundled local plaintext examples. Older engines require TLS and SASL because loopback-published ports may be reachable from the local network.
- GNU Make and Bash for Makefile convenience targets on Unix-like systems.

## Build From Source

```bash
git clone https://github.com/cursus-io/cursus.git
cd cursus
go mod download
make build
```

`make build` creates three binaries for the current OS:

```text
bin/cursus
bin/cursus-cli
bin/cursus-storage
```

Build individually:

```bash
make build-api
make build-cli
make build-storage
```

Cross-compile all three for Linux (the output names end in `-linux`):

```bash
make build-linux
```

Direct broker build:

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/cursus ./cmd/broker
```

There is no standalone `cmd/bench` binary. End-to-end benchmarks use Docker Compose and `test/e2e-benchmark`; storage microbenchmarks use `go test -bench`. See [Benchmarks](../reference/benchmark.md).

## Run From Source

```bash
./bin/cursus
```

or:

```bash
make run
```

Without `--config`/`CONFIG_PATH`, built-in defaults are used. A missing,
unreadable, or malformed explicitly selected configuration file fails startup.

## Container Image

Pull the published GHCR image:

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker run --rm \
  -p 127.0.0.1:9000:9000 -p 127.0.0.1:9080:9080 -p 127.0.0.1:9100:9100 \
  -e BROKER_BIND_ADDRESS=0.0.0.0 \
  -e ALLOW_INSECURE_CLIENT_TRANSPORT=true \
  -v cursus-data:/data/logs \
  ghcr.io/cursus-io/cursus:latest
```

The command above is a development-only plaintext listener. Keep the host bindings on loopback and use Docker Engine 28.3.3 or newer. For shared or production access, enable TLS and SASL and remove `ALLOW_INSECURE_CLIENT_TRANSPORT`.

Use a version tag for repeatable deployment:

```bash
docker pull ghcr.io/cursus-io/cursus:<version>
```

Build locally:

```bash
docker build -t cursus:local .
```

The multi-stage Dockerfile builds `/app/broker`, `/app/cli`, `/app/cursusctl`,
and `/app/cursus-storage` with the Go version declared by the builder image on
Alpine 3.20. `entrypoint.sh` executes `/app/broker`. The image sets
`LOG_DIR=/data/logs`; mount durable storage there. A configuration file is
optional, but when one is selected with `CONFIG_PATH` it must exist and be
readable.

Example configuration mount:

```bash
docker run --rm \
  -p 127.0.0.1:9000:9000 -p 127.0.0.1:9080:9080 -p 127.0.0.1:9100:9100 \
  -e BROKER_BIND_ADDRESS=0.0.0.0 \
  -e CONFIG_PATH=/app/config.yaml \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v cursus-data:/data/logs \
  ghcr.io/cursus-io/cursus:latest
```

## Helm

Two Helm charts are maintained with the broker source:

| Chart | Topology | Storage and upgrade behavior |
|---|---|---|
| `manifests/helm` | Exactly one standalone broker | One log PVC; `Recreate` Deployment upgrades interrupt service |
| `manifests/helm-cluster` | Exactly three Raft members | One PVC per ordinal; explicit `OnDelete` rolling upgrades |

Use the [Kubernetes cluster runbook](../operations/kubernetes-cluster.md) for the
three-member chart, including the required image digest/revision, internal
credentials, certificate SANs, and placement on three nodes. Do not increase
standalone `replicaCount` to create a cluster.

### Standalone installation

Create TLS and SASL Secrets in the release namespace before installing. The
certificate must cover the hostname used by clients; the SASL Secret's `users`
entry uses `principal:token:permission1|permission2` entries separated by commas.
Keep the operator token available to your client through your secret manager.

```bash
kubectl create namespace brokers
kubectl -n brokers create secret tls cursus-tls \
  --cert=broker.crt --key=broker.key
kubectl -n brokers create secret generic cursus-client-auth \
  --from-file=users=./sasl-users.txt

# Set DIGEST to the sha256 digest of the selected published broker image.
helm upgrade --install cursus manifests/helm --namespace brokers \
  --set-string image.digest="$DIGEST" \
  --set tls.enabled=true \
  --set tls.secretName=cursus-tls \
  --set clientSecurity.sasl.secretName=cursus-client-auth \
  --wait --timeout 5m
```

`image.digest` takes precedence over `image.tag`. Unlike the cluster chart,
standalone does not verify the image revision or storage/protocol contract in an
init container; review [upgrade compatibility](../operations/upgrade-and-recovery.md)
before changing images. The chart defaults intentionally refuse to render until
TLS and SASL are configured. Only an isolated development deployment should use
`--set clientSecurity.allowInsecure=true`.

Both charts default to non-root containers, `RuntimeDefault` seccomp, dropped
Linux capabilities, no privilege escalation, a read-only root filesystem, and
no mounted Kubernetes API token. The log volume remains writable. Restrict health
and metrics access to trusted operators; these endpoints do not authenticate
clients. The standalone chart does not install a NetworkPolicy.

The standalone chart requests `250m` CPU and `512Mi` memory. It does not set a
generic memory limit because the safe value depends on workload and recovery
size; measure peak use and set limits in production values. With Prometheus
Operator CRDs installed, `monitoring.enabled=true` creates a metrics Service,
ServiceMonitor, and baseline PrometheusRule alerts. Use `monitoring.labels` for
operator selector labels and tune the alert thresholds before paging.

The standalone chart omits `storageClassName` by default, so Kubernetes uses
the cluster's default StorageClass. Set `persistence.storageClass` to select a
named class. For a pre-provisioned PersistentVolume that deliberately has no
class, set `persistence.classless=true`; it cannot be combined with a named
storage class.

Configuration is mounted from a ConfigMap at `persistence.config.path`; only
`persistence.logDir.size` allocates storage. The unused `persistence.config.size`
and `broker.config.logLevel` values have been removed; use `logLevel` to set
broker logging. `performanceProfile` selects `low-latency`, `balanced`, or
`high-throughput`. Override individual settings with `performance.custom`;
`performance.custom.lingerMS=0` disables the batch linger delay.

Before installing, run `helm lint` and `helm template` with the same values and
Secret names as the install. Templates reference existing Secrets and do not
verify that they exist or contain valid credentials. After a Secret change,
restart the standalone Deployment to reload TLS/SASL credentials:

```bash
kubectl -n brokers rollout restart deployment/cursus
kubectl -n brokers rollout status deployment/cursus --timeout=5m
```

This causes a standalone outage. Follow the cluster runbook's one-member-at-a-time
rotation procedure for the three-member chart.

## Verify

```bash
curl -f http://localhost:9080/live
curl -f http://localhost:9080/ready
curl -f http://localhost:9100/metrics
```

The client port accepts only Wire v2, so raw `nc` text without the required handshake and `CRS2` frame is not a valid protocol check. Use `bin/cursus-cli`, a supported SDK, or the E2E client helpers.

Run local validation:

```bash
go test ./...
make e2e
```

Docker benchmark tests are opt-in and main-push-only in CI:

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

## Clean

```bash
make clean
```

The current target removes files under `bin/` and local coverage outputs. It does not delete arbitrary broker log directories or Docker volumes; remove those intentionally with the matching Compose/volume command.

## Next Steps

- [Configuration](configuration.md)
- [Getting Started](README.md)
- [Architecture](../architecture.md)
- [Security And Observability](../reference/observability.md)
