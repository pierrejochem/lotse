# 30. Client simulation in its own CI job

- Status: Accepted (amends how [ADR 0027](0027-client-simulation-depth-in-ci.md) runs the client simulation; its seed counts stay)
- Date: 2026-10-28

## Context

With transactions (ADR 0029) the client simulation takes about 7.5 minutes at the 2,000 seeds per push of ADR 0027, which brought the CI simulation job back to about 13 minutes per push.

## Decision

The client simulation runs in a CI job of its own, in parallel with the other simulations, at the same depth: 2,000 seeds on pushes and pull requests, 5,000 nightly and on manual runs. The other simulations keep running 5,000 seeds per push in the simulation job.

## Consequences

- A push waits about 7.5 minutes for the slowest job instead of about 13 minutes in total.
- More runner minutes in total: the second job builds the workspace again.
- The seed counts are unchanged, so nothing is checked less often.
