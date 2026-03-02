# Comparison with Other Messaging Systems

This document compares **cursus** with other popular messaging systems to help you understand its positioning, strengths, and trade-offs.

## High-Level Comparison Matrix

| Feature | **cursus** | **Apache Kafka** | **NATS (JetStream)** | **RabbitMQ** | **Redis Streams** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Model** | Log-based (Pull/Stream) | Log-based (Pull) | Pub/Sub & Log | Queue-based (Push) | Log-based (Pull) |
| **Storage** | Disk (mmap, Async) | Disk (Sequential) | Memory/Disk | Memory/Disk | In-memory (Optional Disk) |
| **Ordering** | Guaranteed per Partition | Guaranteed per Partition | Guaranteed | Complex (Consumer-side) | Guaranteed |
| **Complexity** | Very Low (Single Binary) | High (JVM, KRaft/ZK) | Low (Go, Single Binary) | Medium (Erlang) | Low |
| **Throughput** | High (Optimized I/O) | Very High | High | Medium | Very High (In-memory) |
| **Replication** | Raft-based | Leader/Follower | Raft (JetStream) | Quorum Queues | Leader/Follower |
| **Deduplication** | Built-in (30m window) | Idempotent Producer | Consumer-side | Plugin-based | Manual/Scripted |

---

## cursus vs. Apache Kafka

**cursus** is heavily inspired by Kafka's partitioned log architecture but aims for a much smaller operational footprint.

*   **Simplicity:** Kafka is a massive ecosystem requiring JVM and complex configuration. Cursus is a single Go binary with minimal dependencies, ideal for edge computing or small-to-medium-scale environments.
*   **Built-in Deduplication:** Cursus provides a native 30-minute deduplication window, whereas Kafka requires idempotent producers or external state management for exactly-once processing.
*   **Performance:** While Kafka scales to petabytes, Cursus optimizes for single-node or small cluster performance using Linux-specific optimizations like `sendfile` and `fadvise`.

## cursus vs. NATS (JetStream & Core)

NATS distinguishes between **Core NATS** (ephemeral, at-most-once) and **JetStream** (persistent, at-least-once). Cursus takes a unified approach that combines elements of both.

*   **Unified Model:** Unlike NATS which requires choosing between Core and JetStream, Cursus is built from the ground up as a persistent log-based broker. However, it provides "Core-like" performance by using dual-path delivery: messages are dispatched to active consumers immediately (like Core) while simultaneously being batched for disk persistence (like JetStream).
*   **Streaming Semantics:** Cursus follows the Kafka/JetStream "streaming" model where messages are stored in an ordered log. This allows for features like **Replay** (consuming from a specific offset) and **Retention policies**, which are not available in Core NATS.
*   **Push vs. Pull:** 
    *   **NATS Core** is predominantly push-based.
    *   **NATS JetStream** supports both push and pull.
    *   **Cursus** focuses on a high-performance **Stream/Pull model** with consumer groups, providing better flow control and horizontal scalability for heavy workloads.

---

## The "Core vs. Streaming" Distinction in cursus

When comparing to NATS, it's helpful to understand where Cursus sits:

| Feature | NATS Core | NATS JetStream | **cursus** |
| :--- | :--- | :--- | :--- |
| **Persistence** | None (In-memory only) | Disk/Memory | **Disk (Async Batching)** |
| **Delivery Guarantee** | At-most-once | At-least-once | **At-least-once (Idempotent)** |
| **Flow Control** | Limited (Slow Consumer) | Full (Pull/Push) | **Full (Consumer-driven Pull)** |
| **Message Replay** | No | Yes | **Yes (Offset-based)** |
| **Use Case** | Control plane, RPC | Data streaming, Logs | **Lightweight Data Backbone** |

## cursus vs. RabbitMQ

RabbitMQ is a feature-rich, "smart broker" that handles complex routing.

*   **Push vs. Pull:** RabbitMQ pushes messages to consumers, which can overwhelm them if not tuned correctly. Cursus uses a pull/stream model where consumers control the flow, making it more resilient to spikes.
*   **Persistence:** RabbitMQ's performance often drops significantly when messages are persisted to disk. Cursus is designed for disk-first persistence using asynchronous batching, maintaining high throughput even under heavy load.
*   **Resource Usage:** Being written in Go, Cursus generally has a smaller memory footprint and faster startup time than the Erlang-based RabbitMQ.

## cursus vs. Redis Streams

Redis Streams provides a log-like data structure within an in-memory database.

*   **Durability:** Redis is primarily an in-memory store. While it has AOF/RDB persistence, it is not optimized for handling datasets larger than RAM. Cursus is a dedicated message broker that manages disk segments efficiently, allowing it to handle much larger message backlogs than Redis.
*   **Specialization:** Cursus includes built-in features like consumer group rebalancing, partition management, and protocol-level optimizations (like batching) that are native to a broker but must be manually managed or simulated in Redis.

---

## Summary: When to use cursus?

**Choose cursus if you need:**
1.  **Kafka-like semantics** (partitioning, log-based persistence) without the operational complexity of Kafka.
2.  **Lightweight footprint** for edge deployments or microservices where resource efficiency is critical.
3.  **High-performance disk I/O** on Linux environments.
4.  **Built-in idempotency** and deduplication for reliable message delivery.

**Choose something else if you need:**
-   Massive, enterprise-wide event streaming (use Kafka).
-   Complex routing patterns like header-based or topic-exchange routing (use RabbitMQ).
-   Simple, transient in-memory messaging (use Core NATS or Redis).
