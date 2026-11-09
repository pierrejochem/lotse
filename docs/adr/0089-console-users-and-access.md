# 89. Console users and access

- Status: Accepted
- Date: 2026-11-09

## Context

Users, role bindings and session revocation have been broker requests since ADR 0038, 0039 and 0070, and `lotse-client` and the Java SDK send them, but a cluster admin could manage them only from code. The console showed bindings read only. The plan for managing users and certificates in the console has three phases; this ADR covers the first, which needs no change to the broker or the protocol.

The console acts as the signed-in user ([ADR 0019](0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md)) and must get no power of its own. A password set through it must not pass through it either, and `lotse-client` took only a plaintext password: it derived the SCRAM credential itself.

## Decision

**Routes, acting as the user.** Under `/api/v1/clusters/{cluster}/`:
- `GET users`, `POST users` (a name and a credential), `PUT users/{name}/credential`, `DELETE users/{name}` (body `{confirm}`).
- `GET bindings`, `POST bindings`, `DELETE bindings` (body: the binding).
- `POST sessions/revoke` (body `{subject, group, confirm}`).

Each write answers `Done` with a message and the equivalent CLI command. Every call goes through the signed-in user's client, so the broker's roles decide: a user without the role gets 403 with the broker's reason. `GET bindings` falls back to the caller's own bindings (`WhoAmI`) and says `can_edit: false`, as the tenants screen does; `GET users` is refused for anyone but a cluster admin.

**The browser derives the password.** The browser makes a 16-byte salt and derives the credential with WebCrypto: PBKDF2-HMAC-SHA-512 with 4096 iterations, the stored key as SHA-512 of the HMAC of "Client Key", the server key as the HMAC of "Server Key", the password as its UTF-8 bytes, as in `lotse_auth::scram::ScramCredential`. It sends `salt`, `iterations`, `stored_key` and `server_key`; no request has a password field, so the console never holds one. `lotse-client` gains `set_user_credential`, which `set_user_password` now calls. WebCrypto needs https or localhost; elsewhere the dialog says so and does not offer a weaker path.

**Destructive actions ask for the name again**, as purge does ([ADR 0066](0066-console-actions.md)): deleting a user and revoking sessions. The API checks the name too.

**Read only where the cluster says so.** Users and bindings from the node configuration are listed but not changed.

**One page, three tabs**: *Users and access* beside *Tenants and access*, with Users, Role bindings and Sessions. There is no list of open sessions, because the broker has none; the Sessions tab revokes by name.

## Consequences

- A cluster admin manages users, bindings and sessions without a terminal. The demo cluster plays the same flows in memory.
- The broker cannot tell a browser-derived credential from one derived by a client; it checks only the iteration minimum. A browser that derived with fewer iterations is refused by the broker, not by the console.
- Certificates are untouched. Revoking and issuing client certificates are later phases and need broker changes and their own ADRs.
- Tests:
  - the console against a real node that requires authentication: a user created through the console signs in with its password, the old password stops working after a change, a deleted user cannot sign in, a user without the role is refused by the broker, bindings added and removed, the name typed again;
  - the demo cluster's flows and the OpenAPI paths;
  - one shared vector, computed outside both code bases, that the Rust credential and the browser's both reproduce;
  - mutation check: removing the typed-name checks fails the demo test.
