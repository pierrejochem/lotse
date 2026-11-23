<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/readme-hero-dark.png">
    <img src="docs/brand/readme-hero-light.png" alt="Lotse: queues and streams on one replicated log" width="100%">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/pierrejochem/lotse/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/pierrejochem/lotse/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://pierrejochem.github.io/lotse/"><img src="https://img.shields.io/github/actions/workflow/status/pierrejochem/lotse/docs.yml?branch=main&label=docs" alt="Docs"></a>
  <a href="https://github.com/pierrejochem/lotse/releases"><img src="https://img.shields.io/github/v/release/pierrejochem/lotse?include_prereleases" alt="Latest release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <a href="Cargo.toml"><img src="https://img.shields.io/badge/rust-1.85%2B-orange.svg" alt="Rust: 1.85+"></a>
  <a href="AGENTS.md"><img src="https://img.shields.io/badge/unsafe-forbidden-success.svg" alt="Unsafe: forbidden"></a>
</p>

<p align="center">
  <a href="https://pierrejochem.github.io/lotse/"><b>Guide</b></a> ·
  <a href="#quick-start"><b>Quick start</b></a> ·
  <a href="docs/status.md"><b>Status</b></a> ·
  <a href="sdk/"><b>Client SDKs</b></a> ·
  <a href="docs/design.md"><b>Design</b></a>
</p>

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

- **Queues and streams on one log:** RabbitMQ-style queues (acks, redelivery, dead-letter queues, delayed delivery, priorities, exchanges and bindings) and Kafka-style streams (partitions, consumer groups, retention, log compaction) share one storage engine.
- **Replication with Raft:** every partition is a Raft group, batched per node pair; nodes can be added to and drained from a running cluster.
- **Exactly-once:** idempotent producers, transactions and consumer group offsets committed in the same transaction.
- **Secure by default:** TLS 1.3 on every connection, SCRAM-SHA-512, OAUTHBEARER, client certificates and access tokens, tenants and roles.
- **Tested by simulation:** deterministic simulation with crashing disks and a faulty network, 5,000 seeds per simulation before every release.
- **Clients and console:** clients for Rust, Java (with a Kotlin layer), C# and Node.js, a web console, and `lotse mcp` for AI agents.

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

[AGENTS.md](AGENTS.md) holds the rules, commands and conventions for everyone who changes this repository, people and AI coding agents alike; [agents/lotse-engineer.md](agents/lotse-engineer.md) describes the project's dedicated coding agent. Before opening a pull request, run:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Security

Report a vulnerability privately, not in a public issue: [SECURITY.md](SECURITY.md) says how.

## License

Apache License 2.0; see [LICENSE](LICENSE).
