# 📚 Project Documentation

This directory contains various documents explaining different aspects of the project. Please navigate through the list below to find the information you need.

## 📖 Core Document Index

- **[Architecture](architecture.md)** – Overall design and architectural plan.
- **[Core](core/README.md)** – Core components overview, including message flow and server handling.
  - **[Message Flow](core/message-flow.md)** – How messages travel through the broker system.
  - **[Server](core/server.md)** – Server implementation, connection handling, and concurrency model.
  - **Storage**
    - **[Disk Format](core/storage/disk-format.md)** – On-disk data structures.
    - **[Disk Persistence](core/storage/disk-persistence.md)** – Data persistence and durability mechanisms.
    - **[Platform Optimizations](core/storage/platform-optimizations.md)** – Platform-specific optimizations.
    - **[Segment Management](core/storage/segment-management.md)** – Segment handling, rolling, and cleanup.
  - **Topic**
    - **[Consumer Groups](core/topic/consumer-groups.md)** – Management and coordination of consumer groups.
    - **[Topic Management](core/topic/topic-management.md)** – Creation, deletion, and configuration of topics.
    - **[Topics and Partitions](core/topic/topics-and-partitions.md)** – Partitioning and topic-to-partition mapping.
- **[User Guide](user-guide/README.md)** – Introduction and guidance on installation, configuration, and using the broker.
  - **[Installation](user-guide/installation.md)** – Step-by-step installation instructions.
  - **[Configuration](user-guide/configuration.md)** – Configuration options and settings.
- **Reference**
  - **[API Reference](reference/api-reference.md)** – Complete API documentation.
  - **[Comparison with Others](reference/comparison.md)** – Detailed comparison with other message brokers.
  - **[Benchmark](reference/benchmark.md)** – Performance benchmark results and testing methodology.
  - **[Command Interface](reference/command-interface.md)** – CLI commands and usage.
  - **[Observability](reference/observability.md)** – Metrics, logging, and monitoring.
  - **[Performance](reference/performance.md)** – Performance tuning and optimization strategies.
- **[Contributing](contributing/README.md)** – Guidelines for contributing to the project and documentation.

## 📝 Contribution and Maintenance

We welcome any contributions or suggestions to improve our documentation. Please refer to the respective links for detailed information.

- To add new documentation or suggest improvements to existing files, please create a Pull Request.
