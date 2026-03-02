<div align="center">

<img src=".github/cursus-readme.png" alt="cursus" width="60%" height="60%"> 

[![GitHub](https://img.shields.io/github/stars/cursus-io/cursus.svg?style=social)](https://github.com/cursus-io/cursus)
[![Latest Release](https://img.shields.io/github/v/release/cursus-io/cursus?include_prereleases&label=release&color=00ADD8)](https://github.com/cursus-io/cursus/releases)
[![Build Status](https://github.com/cursus-io/cursus/actions/workflows/ci-build.yml/badge.svg?branch=main)](https://github.com/cursus-io/cursus/actions/workflows/ci-build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/cursus-io/cursus)](https://goreportcard.com/report/github.com/cursus-io/cursus)
[![CodeCov](https://img.shields.io/codecov/c/github/cursus-io/cursus)](https://codecov.io/gh/cursus-io/cursus)
![Go Version](https://img.shields.io/github/go-mod/go-version/cursus-io/cursus)
[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/cursus-io/cursus)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg?)](https://github.com/cursus-io/cursus/blob/main/LICENSE)

</div>

<br>

Cursus is a **lightweight message broker** inspired by design philosophy of
_logically separated but physically distributed data management_.

It aims to provide a minimal, efficient, and extensible messaging backbone for small-scale environments.

## 🚀 Key Features
**📨 Topic-based Messaging**
- Parallel processing by partition unit
- Synchronous, asynchronous, and batch-based message publishing with idempotent producers
- Pull/Stream model consumption, consumer groups with automatic rebalancing

**💾 Persistence**
- Asynchronous disk writes with batching
- Segment rotation, efficient reads through mmap

**🛠 Flexibility**
- Platform-specific optimizations (Linux: sendfile, fadvise)
- Stand-alone and Distributed Cluster (Raft) mode

## 📖 Documentation

To learn more about [documentation](docs/README.md).

## 🤝 Community

This project is currently maintained by a single developer.
We truly welcome early contributors and feedback during this development phase.

As the project grows and becomes more mature, we plan to establish more structured community such as a Slack and regular contributor meetings.

For now, you can:
- Ask questions or share feedback via GitHub Issues
- Reach out directly through email or GitHub
