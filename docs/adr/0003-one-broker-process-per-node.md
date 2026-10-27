# 3. One broker process per node

- Status: Accepted
- Date: 2026-10-27

## Context

Splitting routing, storage and replication into separate services adds network hops and failure modes to the data path.

## Decision

A node runs one broker process (`lotse-server`); services are crates and modules inside it. Connectors (databases, HTTP, other messaging systems) run as separate processes that use the native protocol as clients.

## Consequences
- No network hops inside the data path.
- Crate boundaries, not process boundaries, keep the code modular.
