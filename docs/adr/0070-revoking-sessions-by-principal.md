# 70. Revoking sessions by principal

- Status: Accepted
- Date: 2026-11-06

## Context

An OAUTHBEARER token stayed good until it expired (ADR 0038), and an open SCRAM or certificate session lasted until it ended, so a user whose access had to end at once kept it. The maintainer chose to revoke by principal, over asking the identity provider (introspection) or listing single token ids.

## Decision

**`RevokeSessions` (key 50, cluster admin)** names a user or an IdP group and a time, `before_ms`. A time of 0 means now, by the metadata leader's clock. The metadata group keeps, for each user and group, the latest such time; an earlier one never lowers it.

**What counts as before.** Each session has the time its credential dates from:
- a token's `iat`, or time 0 for a token without one;
- the moment of a SCRAM login;
- the moment the connection was made, for a client certificate.

A session of a principal is revoked if the principal, or one of the groups it has (from its token), was revoked at a later time.

**What happens.**
- Every node publishes the revocations it applied to all its connections.
- A connection whose session is revoked closes, also while it waits for nothing.
- A login whose credential dates from before a revocation fails, which refuses old tokens.
- The client then connects again and authenticates afresh:
  - a SCRAM user with the password gets a new session;
  - a token user needs a token issued after the revocation;
  - a certificate user gets in again, since the CRL (ADR 0060) is what keeps a certificate out.
- `Client::revoke_sessions` sends the request.

## Consequences

- An admin can cut a user or a whole IdP group off at once, without help from the identity provider, and the revocation survives restarts and snapshots.
- It revokes every token of the principal issued before the time, not one token.
- To keep a SCRAM user out, change or delete the password; to keep a certificate out, revoke it in the CRL.
- Revocations are kept for ever: a few bytes per user or group.
- The check compares times from different clocks: the metadata leader's, the identity provider's (`iat`), and each node's (SCRAM, certificates). A token issued within a clock skew of the revocation may be refused or let in.
