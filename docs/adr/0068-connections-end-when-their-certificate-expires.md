# 68. Connections end when their certificate expires

- Status: Accepted
- Date: 2026-11-06

## Context

ADR 0060 ends a connection when the certificate its other end showed is revoked, but it left one case open. A connection whose certificate expires stays open until it closes, because only a new handshake checks validity. A node link or client connection can last for months. A certificate that ends after a year should end the connection that proved it, not only the next handshake.

## Decision

**A connection ends when the certificate its other end showed expires.**
- When a TLS connection is handshaken, `lotse-rt-tokio` reads the end of the other end's certificate's validity (`not_after`) and turns it into a deadline on Tokio's clock.
- At that deadline, the connection fails its sends and receives, including one already waiting, with "the other end's certificate expired". This is the same way a revoked connection fails (ADR 0060).
- Every call after that fails as well.
- A connection without a certificate on the other end is not affected. Clients that sign in with SCRAM or a token and show no certificate have no certificate to expire; ADR 0038's session end covers them.

Both ends do this. A node ends a client's connection when the client certificate expires, and a client ends its connection to a node when the node's certificate expires. Then the usual reconnect happens: with renewed files, which a running node picks up within 10 s (ADR 0060), the next handshake succeeds; without them, it fails.

## Consequences

- An expired certificate no longer keeps a connection open. Renewing in time keeps traffic flowing: the old connection ends at the old certificate's expiry, and the next connection uses the new certificate.
- The deadline comes from the machine's clock when the connection is made. If the clock jumps afterwards, the connection still ends at the computed time.
- Certificates count whole seconds; a connection ends at most a second after the last second its certificate is valid in.
