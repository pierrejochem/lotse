# 10. Deterministic simulation and mutation checking

- Status: Accepted
- Date: 2026-10-27

## Context

Distributed-systems bugs live in interleavings, crashes and message orders that example tests do not reach.

## Decision

Correctness-critical code is tested in seeded deterministic simulation (`lotse-sim`): virtual time, a seeded executor, a disk that loses or tears unsynced data on a crash and can lose power mid-operation, and networks that drop, delay, duplicate, reorder and partition. A failing run prints its seed. Every safety rule is mutation-checked: the rule is broken on purpose and some test must fail; a rule no test catches gets a test. CI runs 5,000 seeds per simulation; Jepsen-style tests come before 1.0.

## Consequences
- Many real bugs were found this way before any user could hit them (see ADRs 0017, 0018, 0023).
- Simulations must keep their fault models honest: a check that never fires proves nothing.

See also: [design document](../design.md#simulation).
