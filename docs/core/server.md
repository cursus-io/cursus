# Server System

## Purpose and Scope

This document describes the Server System in cursus, which is responsible for accepting TCP connections, managing concurrent client sessions, and routing commands to the appropriate subsystems. 

The server implements a bounded worker pool pattern to handle connections efficiently and exposes multiple network ports for different purposes.

- For details on message publishing and consumption workflows, see [Publishing Messages and Consuming Messages](./message-flow.md). 
- For configuration of server parameters, see [Configuration](../user-guide/configuration.md).

## Server Architecture Overview

The server system consists of three primary components:

| Component              | Port          | Purpose                                    | Protocol                        |
|------------------------|---------------|--------------------------------------------|--------------------------------|
| Main Broker Server     | 9000 (default)| Message publishing, topic management, consumption | TCP with length-prefixed framing |
| Health Check Server    | 9080 (default)| Readiness/liveness probes                  | HTTP                           |
| Metrics Exporter       | 9100 (default)| Prometheus metrics                          | HTTP                           |

The main broker server is implemented in RunServer and uses a fixed-size worker pool to handle connections concurrently. Each connection is processed by HandleConnection, which handles the message protocol, command detection, and routing.

### Server Components

| Component           | Description |
|--------------------|-------------|
| Metrics Server      | If enabled, starts Prometheus exporter on `cfg.ExporterPort`. |
| TCP Listener        | Creates TLS or plain TCP listener on `cfg.BrokerPort`. |
| Readiness Flag      | Sets `brokerReady` to `true` to signal the health endpoint. |
| Health Check Server | Starts HTTP server for `/health` endpoint. |
| Worker Pool         | Creates worker channel and spawns goroutines. |
| Accept Loop         | Accepts connections and dispatches them to workers. |

## Server Initialization Flow

### Key Constants and Variables:

- `maxWorkers = 1000`: Maximum number of concurrent connection handlers
- `readDeadline = 5 * time.Minute`: Connection read timeout
- `DefaultHealthCheckPort = 9080`: Default health check port
- `brokerReady`: Atomic boolean flag for readiness state

## Worker Pool Pattern

The server uses a bounded worker pool pattern to limit concurrent connections and prevent resource exhaustion.

### Implementation Details:

- **Worker Channel**: `workerCh := make(chan net.Conn, maxWorkers)` 

- **Worker Spawning**: Loop creates 1000 goroutines

- **Blocking Behavior**: When channel is full (1000 pending connections), Accept goroutine blocks
- **Connection Lifecycle**: Each worker processes one connection at a time, then waits for next

Benefits:

- Limits concurrent connections to prevent memory exhaustion
- Provides backpressure when system is overloaded
- Workers are reused across connections

## Connection Handling Protocol

The HandleConnection function processes a single client connection through multiple request-response cycles.

### Message Protocol Format:

Each message uses length-prefixed framing:

```
[4-byte length (big-endian)][message data]
```

### Message Reading and Parsing

#### Length-Prefixed Protocol

The server uses a 4-byte big-endian length prefix for all messages:

- **Read Length**: Read 4 bytes, parse as `binary.BigEndian.Uint32`
- **Read Payload**: Read exactly msgLen bytes
- **Decompress**: Apply gzip decompression if enabled
- **Decode**: Parse as topicName|payload format
- **Read Deadline Management**: Every read operation sets a deadline 

### Message vs Command Detection

The `isCommand` function checks whether the incoming payload begins with a supported command keyword.

| Command   | Purpose                          |
|-----------|----------------------------------|
| `CREATE`  | Create a new topic               |
| `DELETE`  | Delete a topic                   |
| `LIST`    | List all topics                  |
| `SUBSCRIBE` | Subscribe to a topic           |
| `PUBLISH` | Publish a message (explicit)     |
| `CONSUME` | Consume messages from disk       |
| `HELP`    | Show help message                |

If the payload does **not** start with a recognized command, it is treated as a **regular message** to be published to the target topic.

## Command Routing and Response Handling

### Response Types

- Simple Response: Most commands return a string response that's written back to the client
- Stream Response: `CONSUME` command returns `STREAM_DATA_SIGNAL`, triggering streaming mode
- No Response: Direct message publishing returns early after writing "OK"

### Streaming Mode

When `HandleCommand` returns `STREAM_DATA_SIGNAL`:

- Call `HandleConsumeCommand(conn, cmdStr)` to stream messages
- Each message is written with length prefix via `util.WriteWithLength`
- Connection is closed after streaming completes
- No further requests are processed on this connection

### Response Writing

The `writeResponse` function implements the same length-prefixed protocol:

```
respLen := make([]byte, 4)
binary.BigEndian.PutUint32(respLen, uint32(len(resp)))
conn.Write(respLen)      // Write 4-byte length
conn.Write([]byte(resp)) // Write response data
```

This ensures clients can read responses using the same protocol as sending requests.

## Health Check System

The health server binds synchronously on a separate HTTP port. A bind failure aborts broker startup.

### Endpoints

| Endpoint | Meaning | Success |
|---|---|---|
| `/live` | The broker process and health handler are running | `200` JSON |
| `/ready` | Startup completed and dynamic dependency checks pass | `200` JSON |
| `/health` | Backward-compatible readiness check | `200 OK` plain text |
| `/` | Alias of `/health` | `200 OK` plain text |

Readiness becomes true only after the TCP listener, command handler, worker pool, and enabled HTTP services are initialized. Distributed brokers also require a resolvable cluster leader. Leader election can therefore produce `/live = 200` and `/ready = 503`, which keeps the process alive while removing it from client traffic.

See [Broker Observability](../reference/observability.md) for response bodies, metrics, and probe examples.

## Integration with Other Systems

### TopicManager Integration

The TopicManager is passed to each connection handler and is used for:

- Publishing messages: `tm.Publish(topicName, msg)`
- Topic operations via CommandHandler: `create`, `delete`, `list`, `subscribe`

### DiskManager Integration

The DiskManager is passed to CommandHandler and is used for:

- Reading messages from disk during `CONSUME` operations
Managing per-topic-partition DiskHandlers

### CommandHandler Integration

Each connection creates its own CommandHandler instance:

```
cmdHandler := controller.NewCommandHandler(tm, dm)
```

And a ClientContext:

```
ctx := controller.NewClientContext("default-group", 0)
```

The CommandHandler routes commands to appropriate subsystems. See [Command Processing](../reference/api-reference.md) for details.

### Configuration Integration

The `Config` struct provides tunable parameters:

| Parameter        | Default | Purpose |
|-----------------|---------|---------|
| `BrokerPort`     | 9000    | Main TCP server port |
| `HealthCheckPort`| 9080    | HTTP health check port |
| `UseTLS`         | false   | Enable TLS encryption |
| `EnableGzip`     | false   | Enable message compression |
| `EnableExporter` | true    | Enable Prometheus metrics |
| `ExporterPort`   | 9100    | Metrics HTTP port |


## TLS and Compression Support

### TLS Configuration

When `cfg.UseTLS` is true:

```
tlsConfig := &tls.Config{Certificates: []tls.Certificate{cfg.TLSCert}}
ln, err = tls.Listen("tcp", addr, tlsConfig)
```

The certificate is loaded during configuration initialization from `cfg.TLSCertPath` and `cfg.TLSKeyPath`.

### Gzip Compression

When `cfg.EnableGzip` is true, messages are decompressed after reading:

```
data, err := DecompressMessage(msgBuf, enableGzip)
```

The `DecompressMessage` function handles optional gzip decompression.

## Error Handling and Logging

The server uses structured logging for different event types:

### Structured Logging

| Log Prefix      | Purpose                     | Example |
|-----------------|----------------------------|---------|
| `[REQ]`         | Incoming request           | `[REQ] [127.0.0.1:54321] Received request. Topic: 'orders', Payload: 'CREATE orders 4'` |
| `[INPUT_WARN]`  | Malformed input            | `[INPUT_WARN] [127.0.0.1:54321] Received unrecognized input` |
| `[CONSUME_ERR]` | Consume streaming error    | `[CONSUME_ERR] Error streaming data for command [CONSUME orders 0 0]` |
| `[STREAM]`      | Streaming completion       | `[STREAM] Completed streaming 42 messages` |

### Connection Error Handling

| Error Type       | Handling                                                                 | Code Location |
|-----------------|-------------------------------------------------------------------------|---------------|
| EOF             | Silently close connection (normal client disconnect)                    | `pkg/server/main.go` **97–100** |
| Read errors     | Log warning and close connection                                         | `pkg/server/main.go` **98** |
| Decompress errors | Log warning and close connection                                        | `pkg/server/main.go` **112–114** |
| Accept errors   | Log warning and continue accepting                                       | `pkg/server/main.go` **75–77** |


# Summary

The Server System provides the network interface for cursus, implementing:

- **Worker Pool Pattern**: 1000 concurrent workers handle connections efficiently
- **Length-Prefixed Protocol**: 4-byte big-endian length prefix for reliable message framing
- **Dual-Mode Processing**: Commands routed to controller, messages published directly
- **Streaming Support**: Special handling for CONSUME command to stream messages
- **Health Checks**: Separate HTTP endpoint for orchestration
- **Optional Features**: TLS encryption and gzip compression

The server acts as the entry point for all client interactions, routing requests to the Topic Management System and Disk Persistence System as needed.
