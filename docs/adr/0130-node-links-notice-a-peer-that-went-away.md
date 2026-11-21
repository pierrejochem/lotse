# 130. Node links notice a peer that went away

- Status: Accepted
- Date: 2026-11-21
- Changes [ADR 0025](0025-frame-oriented-network-seam-and-raft-wire-format.md) in part: a sender connects anew when the other end closes, not only when a send fails

## Context

Each node keeps one outgoing connection per peer and only writes to it (ADR 0025). It connected anew only when a send failed. On a machine that keeps its address, that is soon enough: the restarted node's kernel answers the old connection with a reset, and the next send fails.

The Helm chart's test (ADR 0128) rolled three nodes on kind, and no group had a leader for ten minutes after the third restart. A replaced pod comes back at another address. Nothing answers at the old one any more: no reset comes back, and the old process's close (its FIN) was never read, since the sender does not read. Every send into the old connection succeeded into the kernel's buffer while TCP retransmitted into nothing, for its retransmission limit of up to about fifteen minutes. After two restarts, every node was sending its Raft messages into connections nobody received, and no group had a quorum. The simulated network broke such a connection at once, like a machine that keeps its address, so no simulation could see this.

A pod killed without closing, or a machine lost with its address, sends no FIN at all. A plain TCP connect to an address nobody answers at waits about two minutes.

## Decision

**A node's sender also reads its outgoing connection and connects anew when the other end closes it. The Tokio runtime fails a connect after 5 s, and a connection whose written data stays unacknowledged for 10 s.**

- **The sender** (`node.rs`, `send_to`) runs its send loop until the connection's read ends. The other end of a node link never writes, so a read ends only with the other end's close or an error. Then it connects anew, which resolves the peer's name again. This holds in the simulation and on Tokio alike.
- **The Tokio runtime** connects within `CONNECT_TIMEOUT` (5 s), and sets `TCP_USER_TIMEOUT` (10 s, Linux) on every connection it makes or accepts, so a write towards an address that went away fails within seconds even when no close arrives.
- **The simulated network** can model this case: with `SimNet::set_silent_close`, a dropped connection end leaves the other end's sends succeeding into nothing, while its reads still see the close. A simulation restarts each node in turn in that mode and requires every group to have a leader within 10 s after each restart. It failed before the change.

Rejected options:

- **TCP keepalive alone.** It probes only an idle connection. Node links are never idle, and unacknowledged writes are what hang.
- **Connecting anew when no frame came from a peer for some time.** A healthy link from a follower that has nothing to send would be dropped. The read side and the unacknowledged-data limit say what is wrong more exactly.

## Consequences

- A node that restarts at another address is heard again as soon as its old process closed its connections, and within about 10 s if it never closed them.
- A link whose peer is merely slow to acknowledge for 10 s is dropped and made anew; Raft sends again what was lost, as after any broken connection.
- On systems other than Linux, only the read side and the connect timeout apply.
- Tests: `a_rolling_restart_survives_old_connections_that_vanish` (simulation), `connections_bound_unacknowledged_data` (Tokio), and the chart's rolling restart on kind in CI.
