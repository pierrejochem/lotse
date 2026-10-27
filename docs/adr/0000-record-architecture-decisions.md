# 0. Record architecture decisions

- Status: Accepted
- Date: 2026-10-27

## Context

Lotse makes many decisions that later code depends on: storage format, replication, protocol, safety rules found by simulation. The design document describes the system as it is; it does not keep the reasons for a choice once the text moves on, nor which alternatives were turned down.

## Decision

Every architecture decision gets a short record in `docs/adr/`, numbered, in this form: Status, Context, Decision, Consequences. A record is never rewritten to say something else; a changed decision gets a new record that supersedes the old one, and the old one's status says so. The design document ([design.md](../design.md)) stays the description of the current system and links here.

## Consequences
- Anyone (person or AI agent) can find why something is the way it is before changing it.
- A pull request that changes a decision adds or supersedes an ADR in the same change.
