# 133. Access tokens with fixed lifetimes

- Status: Accepted
- Date: 2026-11-22

## Context

A script, a CI pipeline or a tool that acts for a person needs a credential that is neither a password nor a certificate. A SCRAM password is the person's own and cannot be taken back without changing it; a client certificate needs the CA holder (ADR 0092) or a renewal CA (ADR 0123); an OIDC token comes from an identity provider that a service account may not have. Operators want something a user makes for themselves in a minute, that stops by itself, that can be listed and revoked, and that a leaked copy cannot extend.

## Decision

- **The token.** A random secret of 32 bytes, drawn by the client that asks through its runtime's `fill_secret`. Its text is `lotse_tok_<id>_<secret>`: `<id>` is 16 hex digits, `<secret>` 43 characters of base64url. The prefix lets secret scanners recognise one. The client sends only the SHA-256 of the secret and shows the text to the user once; the cluster never sees the secret, as it never sees a password (ADR 0089).
- **What the cluster keeps.** Metadata change `CreateToken` (tag 35, snapshot section 19): id, name (1 to 64 bytes), owner, hash, creation time, expiry and who created it. The id is the index of the change, so it is not secret. `RevokeToken` (tag 36) removes one. Expired tokens are dropped when the next token is created (only `CreateToken` prunes them), as expired renewed certificates are (ADR 0123). Both are written only while the cluster version is at least 0.1 (ADR 0127), which is the first version that has them: below it `CreateToken` and `RevokeToken` are refused with `UNSUPPORTED_VERSION` and `DescribeTokens` answers an empty list.
- **Lifetimes.** One of five steps, named by a code: 1 is 7 days, 2 is 30 days, 3 is 90 days, 4 is 180 days, 5 is 365 days. Anything else is `INVALID_REQUEST`. The expiry is the metadata leader's clock at proposal plus the step, written into the change, so every replica holds the same time.
- **Who may.** Any signed-in user makes tokens for themselves; an empty owner means the caller. A cluster admin may name any principal as owner, such as a service account. The name is not checked against known users: OIDC users are not stored, so there is nothing to check against. The token records who created it. A user lists and revokes their own tokens; a cluster admin and a cluster viewer list all of them, and a cluster admin revokes any. A token that is not the caller's to see is unknown to them (`UNKNOWN_RESOURCE`), as everything a principal may not see is (ADR 0039). A session signed in with a token cannot create tokens (`UNAUTHORIZED`), so a leaked token cannot make successors that outlive it.
- **Limits.** At most 50 live tokens per owner and 10,000 in all; past either, `CreateToken` is refused with `INVALID_REQUEST` and a message that names the limit. The leader counts the tokens it has applied, so creates in flight together can pass a limit by the number in flight, as for renewed certificates.
- **Signing in.** SASL mechanism `LOTSE-TOKEN` (session mechanism 4, listed by `DescribeSessions` with the token's id in tagged field 0 of its entry, never the secret). The one message is the token's text. The node looks the id up in its applied metadata state, then compares the SHA-256 of the secret with the stored hash in constant time. Whatever fails (unknown id, wrong secret, expired, revoked) ends with one message, `authentication failed`, and the node closes the connection.
- **The session.** It acts as the owner with the roles bound to the owner's own name; IdP groups do not count, as for certificates. Its credential time is the token's creation, so `RevokeSessions` for the owner (ADR 0070) covers it. It ends when the token expires, at once when the token is revoked or removed (the node closes the connections that signed in with it, as for a revoked certificate, ADR 0090), and when the owner's sessions are revoked. The node also compares the token's expiry with its own wall clock whenever it checks that the token is still held, at least once a second while the connection waits, so a forward step of the wall clock (an NTP step, a resumed virtual machine) cannot let a session outlive its token.
- **Switch.** `[auth] tokens` in the cluster file, on by default. Off, the node does not offer `LOTSE-TOKEN` and its sign-ins fail.
- **Requests.** `CreateToken` (key 66), `DescribeTokens` (key 67) and `RevokeToken` (key 68), all served by the metadata leader from its applied state. The Rust client has `create_token`, `describe_tokens`, `revoke_token` and `Credentials::Token`.
- **Metrics.** Every node reports `lotse_tokens` (live tokens it has applied) and `lotse_token_sign_ins_total{result}` (`ok` or `failed`).

## Consequences

- A token costs one metadata entry and no private key anywhere; the console's backend and the Rust client draw the secret and the cluster stores a hash.
- Failure timing differs between cases, and that is accepted. An unknown or expired id is refused before any hashing; a known id with a wrong secret costs one SHA-256 and a constant-time comparison. Ids are metadata indexes and not secret, so telling an unknown id from a known one reveals nothing a guesser can use: the secret is 256 bits.
- A retry of `CreateToken` after a lost answer may have been carried out already, and the retry then makes a second token under a later id. The first one is the owner's to list and revoke. The client cannot tell the two apart, so it keeps the text of the answer it got.
- A cluster admin can make a token for a name that no one holds; it grants only roles bound to that name.
- A token cannot be narrowed: it carries all roles bound to its owner's own name for its whole lifetime. Cut one off early by revoking it.
- Rolling upgrades work as for other formats (ADR 0127): a cluster at a lower cluster version keeps its old behaviour until `lotse upgrade finalize`.

## Rejected

- **Tokens that carry IdP groups.** The groups an OIDC user had when they made a token would be frozen in it and outlive their membership. A token grants only roles bound to the owner's own name, as a certificate does (ADR 0119). A user whose roles all come from IdP groups gets a token that grants nothing; the console says so.
- **Free lifetimes.** Any number of days invites tokens that never expire in practice and makes the longest-lived credential a policy question for every request. Five steps, at most a year, are easy to explain, to check and to show.
- **The secret drawn by the cluster.** The leader would see every secret and each replica's log would carry it. Drawing it in the client and storing a hash keeps the cluster out of it, as ADR 0089 does for passwords.
- **Tokens that make tokens.** A token that can mint tokens lets a leaked one outlive its own expiry and revocation. A session signed in with a token cannot create tokens.
