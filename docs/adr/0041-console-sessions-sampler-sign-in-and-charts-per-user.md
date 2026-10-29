# 41. Console sessions in a sealed cookie, the sampler's client credentials, charts per user

- Status: Accepted
- Date: 2026-10-29

## Context

ADR 0040 left three gaps. Sessions lived in the console's memory, so a restart signed everyone out, and two consoles behind one address needed sticky sessions. The metrics sampler could sign in only with a SCRAM password, which puts a password into the console's configuration. And the chart history was collected as the sampler and shown to everyone signed in, so a user who may see one tenant saw the whole cluster's throughput.

## Decision

**The session lives in the browser, sealed** (maintainer's choice). The cookie holds the session itself: its id, the principal, the name to show, the refresh token (or, from a provider that gives none, the access token and its expiry) and the session's end, as JSON encrypted and authenticated with AES-256-GCM under a key only the console has (`auth.session_key_file`, 32 random bytes in base64; without it the console makes one at start and warns that a restart signs everyone out). The cookie stays `HttpOnly`, `SameSite=Lax` and `Secure` over HTTPS, and is kept for the session's lifetime (`auth.session_hours`, default 12).

- The console keeps only a cache of access tokens, by the hash of the cookie value. A console that has not seen a cookie, after a restart or behind a load balancer, renews the access token from the refresh token in it.
- Renewals run one at a time, and the cache answers for the old and the new cookie value alike, so a refresh token that the provider rotates is used once. A rotated refresh token reaches the browser in a new cookie on the response; a cookie that holds no session (another key, changed, ended) is cleared.
- Signing out revokes the refresh token at the provider's revocation endpoint (RFC 7009) if it has one, refuses the session's cookies on this console until the session would have ended, and sends the browser to the provider's end-session endpoint.
- Sign-ins in progress (state, PKCE verifier, nonce) stay in memory for ten minutes: a restart in the middle of one makes the user start again.

**The sampler signs in with client credentials** (maintainer's choice). `metrics.client_id` and `metrics.client_secret_file` let the sampler get its own access token from the issuer the brokers trust (`metrics.issuer`, by default `auth.issuer`) with the OAuth client credentials grant, and pass it to the clusters with OAUTHBEARER. It renews the token 30 s before it expires. If the token's subject is not `metrics.principal`, the console warns: the brokers decide by the token. A password stays possible; both at once is refused.

**Charts are filtered per user** (maintainer's choice). The sampler records, per stream and queue, the bytes written, the bytes read (fetched, and for a queue delivered: nodes now count delivered bytes too) and the records written, in columns aligned with the sample times; a resource absent for the whole retention is dropped. A user's chart is the sum over the resources the user may see when asking, from their own `Metadata`. The commit latency covers the whole cluster and is shown only to cluster admins (by `WhoAmI`, asked again after 30 s; an anonymous principal on a cluster without authentication may do everything). The sampler holds a viewer role in every tenant, bound by an admin; it gets no admin role (since ADR 0043: the `cluster-viewer` role, one binding for all tenants).

## Consequences

- A console restart, or a second console with the same key, keeps users signed in. Rotating the key signs everyone out.
- The refresh token now travels to the browser, encrypted. Someone who copies a cookie holds the session until it ends or is signed out; signing out revokes the token at providers that support revocation. A copy taken before a rotation stops working once the provider refuses the old token.
- A large refresh token or access token can bring the cookie near the 4 KiB browsers keep; the console warns from 3,800 bytes.
- A resource the sampler may not see has no history. A user who gains or loses a role sees the history of what they may see now, including the past.
- Chart memory grows with the number of resources: about 100 KiB per resource for 24 h at 10 s (three 4-byte values per sample).
- The sign-in test now also restarts the console (the session survives, with two requests renewing at once and a rotated refresh token reaching the browser), presents a cookie under another key and a changed one, checks the revocation on sign-out and that no cookie of the session works afterwards even where the provider would still renew it, signs the sampler in with client credentials, and compares a viewer's chart with an admin's. A unit test checks that a session past its end is over. Every rule was broken on purpose and a test failed, except one equivalent mutant: accepting a cookie that does not authenticate yields no plaintext, so nothing parses.
