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

Without `-config`/`CONFIG_PATH`, built-in defaults are used. A missing explicitly named file logs a warning and falls back to effective flag defaults; malformed readable configuration fails startup.

## Container Image

Pull the published GHCR image:

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker run --rm \
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -v cursus-data:/root/broker-logs \
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

The multi-stage Dockerfile builds `/root/broker` and `/root/cli` with Go 1.25.0 on Alpine 3.20. `entrypoint.sh` executes the broker with `-config ./config.yaml`; if no file is mounted, the broker uses defaults after warning. Production deployments should mount a configuration and durable log volume explicitly.

Example configuration mount:

```bash
docker run --rm \
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -v "$PWD/config.yaml:/root/config.yaml:ro" \
  -v cursus-data:/root/broker-logs \
  ghcr.io/cursus-io/cursus:latest
```

## Helm

A Helm chart is available under `manifests/helm`. Review `values.yaml`, persistent volume settings, TLS/internal mTLS secrets, advertised addresses, replica/quorum values, and resource limits before installing. Do not treat chart defaults as a production security profile.

## Verify

```bash
curl -f http://localhost:9080/live
curl -f http://localhost:9080/ready
curl -f http://localhost:9100/metrics
```

The client port is a length-prefixed TCP protocol, so raw `nc` text without the 4-byte frame is not a valid protocol check. Use `bin/cursus-cli`, a supported SDK, or the E2E client helpers.

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
