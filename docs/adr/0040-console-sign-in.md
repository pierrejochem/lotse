# 40. Console sign-in

- Status: Accepted
- Date: 2026-10-28

## Context

ADR 0019 decided that the console acts with the signed-in user's identity and never with rights of its own; ADRs 0038 and 0039 gave the brokers authentication and roles. Until now the console only had `dev` mode, refused OIDC at startup and reached a real cluster without credentials, so against a cluster that requires authentication it could do nothing, and against one that does not, everyone looked the same.

## Decision

**Two sign-in modes** (maintainer's choice of OIDC login and dev SCRAM):

- `oidc`: the console is an OpenID Connect client of the issuer the brokers trust. `/auth/login` sends the browser to the issuer's authorization endpoint (from its discovery document) with Authorization Code and PKCE (S256), a `state` and a `nonce`; `/auth/callback` exchanges the code at the token endpoint and checks the ID token's issuer, audience and nonce. The ID token comes straight from the token endpoint over TLS, so its claims are checked but not its signature (OpenID Connect Core 3.1.3.7); the brokers check the access token's signature themselves. A sign-in is used once and expires after ten minutes, and only returns to a path on the console. `principal_claim` (default `sub`) names the principal and must match the brokers' setting. A confidential client may add `client_secret`; a private issuer's CA goes in `ca_file`.
- `dev`: every request acts as one configured user, with that user's SCRAM password if one is configured. For a machine only you can reach.

**Sessions stay on the console.** The browser holds an opaque, random session id in an `HttpOnly`, `SameSite=Lax` cookie (`Secure` when the console is reached over HTTPS); the tokens never reach the browser. The console renews an access token with the refresh token 30 s before it expires; if it cannot, the session ends and the API answers `401`, upon which the web app sends the browser to `/auth/login` and back. `POST /auth/logout` ends the session and, if the issuer offers it, sends the browser to its end-session endpoint. An open WebSocket checks every 15 s that its session still holds and closes when it does not. A public console needs an `https://` redirect URL; plain HTTP is accepted only on loopback.

**One seat per principal.** A real cluster keeps one client per principal and way of signing in (for a password, its hash): OAUTHBEARER with the user's newest access token (a newer token never gives way to an older one held by a long-lived view), or SCRAM with the password. Samples, and with them rates, are kept per seat, so every screen shows what that principal may see and nothing else. Seats unused for 15 minutes are closed.

**Tenants are shown as the cluster reports them.** Streams and queues carry their tenant; the tenants screen lists the tenants the user holds a role in (`DescribeTenants`), with every role binding for a cluster admin and the user's own bindings (`WhoAmI`) for everyone else. A queue is created in the tenant the user picks. The cluster has no quotas, so a tenant's quota is optional in the API.

**The metrics sampler signs in like a user**: `metrics.principal` with `metrics.password` (SCRAM). It should hold a viewer role; the chart history it collects is shown to everyone signed in to the console.

## Consequences

- A refused request (`UNAUTHORIZED`) or refused sign-in at the cluster reaches the browser as `403` with the cluster's reason; a missing or ended session as `401`.
- Sessions live in the console's memory: restarting the console signs everyone out, and two consoles behind one address need sticky sessions.
- Chart history is collected as one principal, not filtered per user: someone who sees fewer resources still sees the cluster-wide throughput charts. Give the sampler a role whose view everyone may share.
- The metrics sampler cannot sign in with OIDC (no client credentials grant yet).
- A test runs a node that requires authentication, a fake identity provider over HTTP, and the console: a full OIDC sign-in with PKCE and a token renewal, two users at once each seeing only their own, a viewer refused a create, sign-out, refused ID tokens (wrong nonce, audience, issuer), a forged and a replayed state, returns to other sites, and dev mode with a right and a wrong password. Each rule was broken on purpose and the test failed: the nonce, audience and issuer checks, the PKCE verifier, single use, both return-path checks, ending the session, renewal, `HttpOnly`, and the seat keys (principal, password).
