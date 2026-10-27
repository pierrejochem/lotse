# 19. Management console on a dedicated node with the user's identity

- Status: Accepted
- Date: 2026-10-27

## Context

A console with its own admin rights would bypass broker RBAC and blur the audit trail.

## Decision

`lotse-console` is a separate Rust (axum) binary on its own node. It serves the React + TypeScript + Radix app, a REST/JSON API described with OpenAPI (TypeScript client generated) and one WebSocket with subscriptions. Users sign in with OIDC; their token reaches the cluster via SASL OAUTHBEARER, so the broker decides and the audit log shows the real user. Live data is polled once per topic and user and fanned out to all their tabs; history is a 24 h in-memory ring buffer filled by a read-only metrics identity (`metrics.principal`).

## Consequences
- No privileged console account.
- Load grows with users per topic, not with open tabs.

See also: [design document](../design.md#management-console).
