# Stage 1:
FROM golang:1.25.0 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/broker ./cmd/broker \
	&& CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cli ./cmd/cli \
	&& CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cursusctl ./cmd/cursusctl \
	&& CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cursus-storage ./cmd/storage

# Stage 2:
FROM alpine:3.20

RUN apk add --no-cache bash curl
RUN addgroup -g 1000 cursus && adduser -D -u 1000 -G cursus cursus

WORKDIR /app
COPY --from=builder --chown=cursus:cursus /app/broker /app/broker
COPY --from=builder --chown=cursus:cursus /app/cli /app/cli
COPY --from=builder --chown=cursus:cursus /app/cursusctl /app/cursusctl
COPY --from=builder --chown=cursus:cursus /app/cursus-storage /app/cursus-storage
COPY --chown=cursus:cursus entrypoint.sh /app/entrypoint.sh

RUN mkdir -p /data/logs && chown -R cursus:cursus /app /data && chmod +x /app/broker /app/cli /app/cursusctl /app/cursus-storage /app/entrypoint.sh
USER cursus

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
