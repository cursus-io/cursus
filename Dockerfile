# Stage 1:
FROM golang:1.25.0 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cli ./cmd/cli
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cursus-storage ./cmd/storage

# Stage 2:
FROM alpine:3.20

WORKDIR /root/
COPY --from=builder /app/broker .
COPY --from=builder /app/cli .
COPY --from=builder /app/cursus-storage .

RUN apk add --no-cache bash curl

RUN chmod +x broker cli cursus-storage

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/root/entrypoint.sh"]
CMD []