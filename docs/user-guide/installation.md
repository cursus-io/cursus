# Installation

## Requirements

- Go 1.25.0 or newer for source builds (`go.mod` is authoritative).
- Docker Engine with Compose for containerized E2E and benchmark workloads.
- GNU Make and Bash for Makefile convenience targets on Unix-like systems.

## Build From Source

```bash
git clone https://github.com/cursus-io/cursus.git
cd cursus
go mod download
make build
```

`make build` creates two binaries for the current OS:

```text
bin/cursus
bin/cursus-cli
```

Build individually:

```bash
make build-api
make build-cli
```

Cross-compile both for Linux:

```bash
make build-linux
```

Direct broker build:

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/cursus ./cmd/broker
```

There is no standalone `cmd/bench` binary. End-to-end benchmarks use Docker Compose and `test/e2e-benchmark`; storage microbenchmarks use `go test -bench`. See [Benchmark Verification](../benchmark-verification.md).

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
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -v cursus-data:/data/logs \
  ghcr.io/cursus-io/cursus:latest
```

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
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -e CONFIG_PATH=/app/config.yaml \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v cursus-data:/data/logs \
  ghcr.io/cursus-io/cursus:latest
```

## Helm

A Helm chart is available under `manifests/helm`. Review `values.yaml`, persistent volume settings, TLS/internal mTLS secrets, advertised addresses, replica/quorum values, and resource limits before installing. Do not treat chart defaults as a production security profile.

The standalone chart omits `storageClassName` by default, so Kubernetes uses
the cluster's default StorageClass. Set `persistence.storageClass` to select a
named class. For a pre-provisioned PersistentVolume that deliberately has no
class, set `persistence.classless=true`; it cannot be combined with a named
storage class.

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
