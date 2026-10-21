FROM golang:1.25.0 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go test -tags=e2e_faults -c -o /broker-tests ./cmd/broker

FROM debian:bookworm-slim
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    qemu-system-x86 linux-image-amd64 busybox-static cpio kmod e2fsprogs python3 \
    && rm -rf /var/lib/apt/lists/*
COPY test/power-loss/make-initramfs.sh /make-initramfs.sh
COPY test/power-loss/init /guest-init
COPY test/power-loss/run.py /run.py
COPY --from=builder /broker-tests /broker-tests
RUN sh /make-initramfs.sh
ENTRYPOINT ["python3", "/run.py"]
