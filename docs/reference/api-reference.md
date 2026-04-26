# API Reference

This document provides a complete reference for the cursus command API. It describes the wire protocol format, command syntax, parameters, behavior, and responses for all supported operations. The cursus API is a text-based protocol transmitted over TCP with length-prefixed message framing.

For information about the on-disk storage format, see [Disk Format](../core/storage/disk-format.md).

# Wire Protocol Format
All messages exchanged between clients and the broker follow a length-prefixed protocol over TCP.

Each message consists of:

- 4-byte length prefix (big-endian unsigned 32-bit integer)
- Message payload (variable length)

## Message Encoding
Messages are encoded as:

- Topic field: The topic name or command type
- Payload field: The command string or message content

The `util.DecodeMessage()` function splits the raw bytes into topic and payload components. Command detection occurs by checking if the payload starts with command keywords: CREATE, DELETE, LIST, SUBSCRIBE, PUBLISH, CONSUME, or HELP.

## Optional Compression
If gzip compression is enabled via configuration (enable_gzip: true), messages are compressed before length-prefixed framing. 

The DecompressMessage function handles decompression transparently.

## Command Dispatch Flowchart

```mermaid
flowchart TD
    CLIENT([Client TCP Connection]) -->|"[4-byte len][payload]"| HC[HandleConnection]
    HC --> DEC[DecodeMessage\ntopic + payload]
    DEC --> IC{isCommand?\nCREATE/DELETE/LIST\nSUBSCRIBE/PUBLISH\nCONSUME/HELP}

    IC -->|"yes"| CMD[HandleCommand]
    IC -->|"no → data frame"| DATA[Route to topic\nby topic field]

    CMD --> PARSE{Parse first\ntoken}

    PARSE -->|CREATE| CRT["CreateTopic(name, partitions)\n→ TopicManager\n→ Response: OK / ERROR"]
    PARSE -->|DELETE| DEL["DeleteTopic(name)\n→ TopicManager\n→ Response: OK / ERROR"]
    PARSE -->|LIST| LST["ListTopics()\n→ TopicManager\n→ comma-separated names"]
    PARSE -->|SUBSCRIBE| SUB["RegisterConsumerGroup\n→ StreamManager\n→ Response: Subscribed"]
    PARSE -->|PUBLISH| PUB["TopicManager.Publish\n→ dedup → partition\n→ disk + consumers"]
    PARSE -->|CONSUME| CON["STREAM_DATA_SIGNAL\n→ HandleConsumeCommand\n→ DiskHandler read\n→ length-prefixed stream"]
    PARSE -->|HELP| HLP["writeResponse(helpText)"]
    PARSE -->|unknown| ERR["writeResponse\nERROR: unknown command"]

    CRT --> LOG[logCommandResult\nSUCCESS / FAILURE]
    DEL --> LOG
    LST --> LOG
    SUB --> LOG
    PUB --> LOG
    CON --> LOG
    HLP --> LOG
    ERR --> LOG

    LOG --> HC
```

## Command Reference

### 1. CREATE
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `CREATE <topic> [<partitions>]`                                              |
| Parameters   | topic (string, required) - Topic name to create <br> partitions (integer, optional, default=4) - Number of partitions |
| Behavior     | Adds partitions if topic exists <br> Each partition has 10,000-message buffer and dedicated DiskHandler<br>Partition count must be positive |
| Response     | `✅ Topic '<topic>' now has <N> partitions`                                  |
| Errors       | `ERROR: missing topic name`<br>`ERROR: partitions must be a positive integer`  |
| Example      | `CREATE orders 8`<br>`> ✅ Topic 'orders' now has 8 partitions`                  |

### 2. DELETE
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `DELETE <topic>`                                                              |
| Parameters   | topic (string, required) - Topic name to delete                             |
| Behavior     | Removes topic from `TopicManager.topics`<br>Stops partition goroutines<br>Closes consumer channels<br>Disk segment files not automatically deleted |
| Response     | `🗑️ Topic '<topic>' deleted`                                                  |
| Errors       | `ERROR: topic '<topic>' not found`                                            |
| Example      | `DELETE orders`<br>`> 🗑️ Topic 'orders' deleted`                                 |

### 3. LIST
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `LIST`                                                                        |
| Parameters   | None                                                                        |
| Behavior     | Returns comma-separated list of all topic names                             |
| Response     | `<topic1>, <topic2>, <topic3>`<br>`If none exist: (no topics)`                 |
| Example      | `LIST`<br>`> orders, payments, notifications`                                     |

### 4. SUBSCRIBE
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `SUBSCRIBE <topic>`                                                           |
| Parameters   | topic (string, required) - Topic name to subscribe to                       |
| Behavior     | Registers client in topic<br>Creates consumer with 1,000-message buffer<br>Partitions distribute messages using modulo arithmetic<br>Tracks subscribed topics in ClientContext.CurrentTopics |
| Response     | `✅ Subscribed to '<topic>'`                                                  |
| Errors       | `ERROR: topic '<topic>' does not exist`                                       |
| Example      | `SUBSCRIBE orders`<br>`> ✅ Subscribed to 'orders'`                                |

### 5. PUBLISH
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `PUBLISH <topic> <message>`                                                   |
| Parameters   | topic (string, required)<br>message (string, required) - payload           |
| Behavior     | Assigns unique ID via `util.GenerateID`<br>Deduplication check (30 min)<br>Routes to partition (key-based or round-robin)<br>Writes to disk asynchronously<br>Distributes to consumers |
| Response     | `📤 Published to '<topic>'`                                                   |
| Errors       | `ERROR: invalid PUBLISH syntax`<br>`ERROR: topic '<topic>' does not exist`     |
| Example      | `PUBLISH orders {"order_id":123,"amount":99.99}`<br>`> 📤 Published to 'orders'` |

### 6. CONSUME
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `CONSUME <topic> <partition> <offset>`                                       |
| Parameters   | topic (string, required)<br>partition (integer, required, 0-indexed)<br>offset (integer, required, byte offset in segment) |
| Behavior     | Detected by HandleCommand → STREAM_DATA_SIGNAL<br>Handled by HandleConsumeCommand<br>Reads messages from DiskHandler (up to 8192 bytes)<br>Streams back with `util.WriteWithLength` |
| Response     | Each message as `[4-byte length][message payload]`<br>Streamed count logged: `[STREAM]` Completed streaming N messages |
| Errors       | `ERROR: invalid CONSUME syntax`<br>`ERROR: invalid partition ID`<br>`ERROR: invalid offset`<br>`ERROR: failed to get disk handler`<br>`ERROR: failed to read messages from disk` |
| Example      | `CONSUME orders 0 0`<br>`> [4 bytes: 45][message payload 1]`<br>`[4 bytes: 52][message payload 2]` |

### 7. HELP
| Aspect       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Syntax       | `HELP`                                                                        |
| Parameters   | None                                                                        |
| Response     | Lists all available commands with syntax                                     |
| Example      | `HELP`<br>`> Available commands:`<br>`CREATE <topic> [<partitions>]`<br>`DELETE <topic>`<br>`LIST`<br>`SUBSCRIBE <topic>`<br>`PUBLISH <topic> <message>`<br>`CONSUME <topic> <pID> <offset>`<br>`HELP`<br>`EXIT` |

## Response Format

### Simple Commands

- Commands other than CONSUME return a simple string response:
    ```text
    [4-byte length][response string]
    ```
- The writeResponse function handles this encoding:
    ```text
    Streaming Commands (CONSUME)
    ```
- CONSUME returns multiple length-prefixed messages:
    ```
    [4-byte length 1][message 1][4-byte length 2][message 2]...
    ```

## Error Handling

### Error Response Format

All errors are returned as strings prefixed with ERROR:

```
ERROR: <error description>
```

### Common Error Scenarios

| Error                         | Cause                                | Example                       |
|-------------------------------|--------------------------------------|-------------------------------|
| `ERROR: empty command`           | Empty command string received         | -                             |
| `ERROR: unknown command`         | Command not in supported list         | `ERROR: unknown command: FOO`   |
| `ERROR: missing topic name`      | CREATE without topic name             | `CREATE`                        |
| `ERROR: topic '<topic>' not found` | Operation on non-existent topic     | `DELETE nonexistent`            |
| `ERROR: topic '<topic>' does not exist` | SUBSCRIBE/PUBLISH to missing topic | `SUBSCRIBE nonexistent`         |
| `ERROR: invalid PUBLISH syntax` | PUBLISH without message               | `PUBLISH orders`                |
| `ERROR: invalid CONSUME syntax` | CONSUME with wrong argument count     | `CONSUME orders`                |


### Error Logging
The `logCommandResult` function logs all command outcomes:

```
[CMD] SUCCESS for successful commands
[CMD] FAILURE for error responses
```

### Client Context
Each connection maintains a `ClientContext` that tracks:

| Field         | Type                 | Description                                         |
|---------------|--------------------|-----------------------------------------------------|
| ConsumerGroup | string              | Consumer group identifier (e.g., "tcp-group")      |
| ConsumerID    | int                 | Consumer identifier within group                   |
| CurrentTopics | map[string]struct{} | Set of subscribed topic names                      |

## Implementation References

### Key Functions

| Function               | Purpose                           |
|------------------------|-----------------------------------|
| HandleConnection        | Main connection handler loop       |
| HandleCommand           | Command parser and dispatcher      |
| HandleConsumeCommand    | CONSUME streaming handler          |
| isCommand               | Command keyword detection          |
| writeResponse           | Response encoding                  |
| logCommandResult        | Command logging                    |

### Configuration Parameters
Commands respect these configuration parameters:

| Parameter            | Config Field                  | Default    | Impact                              |
|---------------------|-------------------------------|-----------|-------------------------------------|
| Partition buffer size | `partition_channel_buffer_size` | 10000     | CREATE command partition capacity    |
| Consumer buffer size  | `consumer_channel_buffer_size`  | 1000      | SUBSCRIBE consumer channel capacity |
| Read batch size       | N/A               | 8192 bytes| CONSUME read size                   |

