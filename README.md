# Lotse

<p><img src="docs/brand/logos/lotse-wordmark.svg" alt="Lotse" height="56"></p>

[![CI](https://github.com/pierrejochem/lotse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pierrejochem/lotse/actions/workflows/ci.yml)
[![Docs](https://github.com/pierrejochem/lotse/actions/workflows/docs.yml/badge.svg?branch=main)](https://pierrejochem.github.io/lotse/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust: 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](Cargo.toml)
[![Unsafe: forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](AGENTS.md)
[![Latest release](https://img.shields.io/github/v/release/pierrejochem/lotse?include_prereleases)](https://github.com/pierrejochem/lotse/releases)

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

- **Queues and streams on one log:** RabbitMQ-style queues (acks, redelivery, dead-letter queues, delayed delivery, priorities, exchanges and bindings) and Kafka-style streams (partitions, consumer groups, retention, log compaction) share one storage engine.
- **Replication with Raft:** every partition is a Raft group, batched per node pair; nodes can be added to and drained from a running cluster.
- **Exactly-once:** idempotent producers, transactions and consumer group offsets committed in the same transaction.
- **Secure by default:** TLS 1.3 on every connection, SCRAM-SHA-512, OAUTHBEARER and client certificates, tenants and roles.
- **Tested by simulation:** deterministic simulation with crashing disks and a faulty network, 5,000 seeds per simulation before every release.
- **Clients and console:** clients for Rust, Java (with a Kotlin layer), C# and Node.js, and a web console.

Status: foundation. What exists, what is not built yet and the limits that were decided are in [Status and known gaps](docs/status.md).

## Quick start

```sh
cargo build --release -p lotse-node                               # target/release/lotse
lotse check --config cluster.toml                                 # what the file describes
lotse certs --config cluster.toml                                 # a CA and a certificate per node
lotse certs --config cluster.toml --client alice --out ./alice    # the first admin's client certificate
lotse run --config cluster.toml --node 1                          # on each machine, its own id
```

The cluster file, certificates, adding and draining nodes, packages and images: [Running a cluster](docs/running.md). For a cluster on one machine, see [a one-machine trial](docs/guide/src/setup/one-machine-trial.md).

## Documentation

- [Guide](https://pierrejochem.github.io/lotse/): setup, administration, client development and use.
- [Running a cluster](docs/running.md): the cluster file, certificates, nodes, packages, images and systemd in short.
- [Status and known gaps](docs/status.md): what exists, what is not built yet, and the limits by design.
- [Crates](docs/crates.md): what each crate of the workspace does.
- [Testing](docs/testing.md): the simulations, what they check and the bugs they catch.
- [Management console](docs/console/README.md): screenshots, running it and frontend work.
- [Client SDKs](sdk/): the protocol for implementers, golden test vectors and the Java (with a Kotlin layer), C# and Node.js clients.
- [Design document](docs/design.md): architecture and storage design with the reasons behind each decision (German).
- [Architecture decisions](docs/adr/): one record per decision, with its reasons.
- [Architecture overview](docs/architecture.png): the whole system on one page.
- [Brand](docs/brand/): the Fairway mark, logos, colours, type and design tokens.

## Contributing

[AGENTS.md](AGENTS.md) holds the rules, commands and conventions for everyone who changes this repository, people and AI coding agents alike; [agents/lotse-engineer.md](agents/lotse-engineer.md) describes the project's dedicated coding agent.

```sh
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```
