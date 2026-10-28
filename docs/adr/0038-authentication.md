# 38. Authentication

- Status: Accepted
- Date: 2026-10-28

## Context

With TLS (ADR 0037) a node knows which connections come from nodes, and clients may show a certificate, but most users are people and services that log in with a password or a token from the organisation's identity provider. ADR 0004 named SASL with SCRAM-SHA-512 and OAUTHBEARER; ADR 0019 has the console pass the signed-in user's token on. Nothing checked who a client was, and everything a node does must stay reproducible in the simulation: no clock, socket or randomness outside the runtime seam.

## Decision

**Three ways in** (maintainer's choice of the full design):

- a **client certificate** from the cluster's CA names its principal by common name, when `mtls` is on; a node certificate makes the connection the node itself;
- **SASL SCRAM-SHA-512** (RFC 5802, RFC 7677) with a user name and password;
- **SASL OAUTHBEARER** (RFC 7628) with an OpenID Connect access token.

`SaslHandshake` (30) chooses the mechanism, `SaslAuthenticate` (31) carries its steps. When `required` is set, a connection may send nothing else but `ApiVersions` until it is authenticated; anything else is answered with `UNAUTHORIZED` (13) at the frame level. A failed attempt is answered with `AUTHENTICATION_FAILED` (27), which says no more than that for SCRAM so names cannot be probed, and the connection closes. An unknown mechanism gets `UNSUPPORTED_SASL_MECHANISM` (28) and the list of enabled ones.

**Credentials live in the metadata group** (maintainer's choice). `AlterUser` (32) sets or removes a user's SCRAM credential; `DescribeUsers` (33) lists users. The broker keeps salt, iteration count, `StoredKey` and `ServerKey`, never the password, and the client derives them, so the password does not leave it either. Every node applies the metadata group and checks logins against its applied state without asking anyone; the snapshot format becomes 8. Users in the node configuration cannot be changed at runtime and win over runtime users of the same name.

**Tokens are checked against the issuer's keys** (maintainer's choice). A node fetches the issuer's discovery document and key set itself, through a new `Http` seam (`GET` only, 1 MiB at most, HTTPS against the web's roots, plain HTTP only on loopback), and caches the keys. A token with an unknown key id makes it fetch again, at most every 30 s, so made-up key ids cannot flood the provider. A token must be signed with an asymmetric algorithm by a key of the set, name the issuer and the configured audience, not be expired (60 s of slack for clocks), and name the principal in the configured claim (`sub` by default); the groups claim (`groups`) goes with the principal for authorization. The time comes from a new `Clock::unix_time`, fixed per seed in the simulation.

**Sessions end.** A SASL session lasts `max_session` (1 h by default), a token's no longer than the token. The client authenticates again on the same connection after four fifths of it; after the end, requests are refused until it does. Authenticating again cannot change the principal: such an attempt fails and closes the connection. A removed user or revoked token is thus cut off within `max_session`.

**Authentication runs in the connection's task.** The main loop only ever sees a request together with its principal; the SASL requests never reach it.

**Authentication needs TLS.** Nodes reach each other's client ports too (transaction markers) and can only prove who they are with their certificates, so a node refuses to start with `required` on a network that does not verify peers.

**Secrets come from the operating system.** Nonces and salts come from a new `Rng::fill_secret`, the operating system's random source in production (the general `next_u64` need only be well spread), the seed under simulation.

## Consequences

- `lotse-auth` holds the mechanisms, free of I/O: SCRAM for both ends, OAUTHBEARER messages and token validation, and (feature `testing`) a fake issuer that signs tokens deterministically.
- `lotse-client` takes `Credentials` (SCRAM or a function returning the current token), authenticates each new connection, renews sessions, fails at once with `ClientError::Authentication` instead of retrying a login that cannot succeed, and sets and removes users.
- A simulation runs three nodes that require authentication with a fake identity provider: no request before authenticating, each mechanism letting the right principals in and the wrong ones out, users created, changed and removed at runtime, keys rotated, sessions renewed by clients and ended by nodes, no change of principal, transaction markers between nodes. Tests on real sockets do the same over TLS, with the identity provider over HTTP. Each rule was broken on purpose and a test failed, except three that are backed up so that breaking one alone changes nothing: refusing symmetric token algorithms and refusing a key whose algorithm differs from the token's (the token library also refuses a key of the wrong kind), and refusing an unknown SCRAM user outright (the made-up salt it was sent makes its proof fail anyway).
- Found on the way: a response queued just before the connection task closed (the answer to a failed login) could be dropped, because the writer stopped as soon as it saw the close; it now sends what is queued first.
- A rotated key is unknown for up to 30 s if the node fetched the keys just before; identity providers publish new keys ahead of using them, which hides this.
- Every authenticated principal may still do everything: who may do what follows in the next ADR.
- Not built: SCRAM-SHA-256, PLAIN, Kerberos, token introspection for opaque tokens, and checking that a token was revoked before it expires.
