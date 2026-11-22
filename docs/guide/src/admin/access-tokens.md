# Access tokens

An access token lets a script, a pipeline or a tool sign in as a user without that user's password or a certificate. It stops by itself, can be listed and revoked, and can be made by the user who will use it ([ADR 0133](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0133-access-tokens-with-fixed-lifetimes.md)).

A token looks like this:

```
lotse_tok_00000000000001f4_Qm9yZWQgb2YgcmVhZGluZz8gR29vZCBlbm91Z2g
```

The `lotse_tok_` prefix lets secret scanners find one that leaked. The 16 hex digits are the token's id, which `DescribeTokens` lists; the rest is the secret. The cluster keeps only the SHA-256 of the secret, so a token is shown once, when it is made, and cannot be shown again. Lose it and make a new one.

## Switch on or off

`[auth] tokens` in the cluster file, on by default. Off, a node does not offer the `LOTSE-TOKEN` mechanism and no token signs in; the tokens already made stay in the cluster and work again when it is back on. See [the cluster file](cluster-file.md#auth).

```toml
[auth]
tokens = true
```

## Cluster version 0.1

Tokens are a format of release 0.1.0, written only while cluster version 0.1 is in force ([ADR 0127](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)). Below it, making and revoking a token fails with `UNSUPPORTED_VERSION` and listing answers an empty list. After every node runs release 0.1.0 or later, raise the version:

```sh
lotse upgrade status --config cluster.toml --cert admin.pem --key admin.key
lotse upgrade finalize --config cluster.toml --cert admin.pem --key admin.key
```

See [Upgrades](upgrades.md).

## Lifetimes

A token lasts exactly one of five times, counted from when the metadata leader records it:

| Step | Lifetime |
| --- | --- |
| 1 (`Lifetime::Week`) | 7 days |
| 2 (`Month`) | 30 days |
| 3 (`Quarter`) | 90 days |
| 4 (`Half`) | 180 days |
| 5 (`Year`) | 365 days |

Any other value is refused with `INVALID_REQUEST`. A token cannot be extended; make a new one before the old one ends. Expired tokens leave the list.

## Who may

| Action | Who |
| --- | --- |
| Make a token | Any signed-in user for themselves. A cluster admin also for any principal, such as a service account or a pipeline. |
| List tokens | A user their own. A cluster admin and a cluster viewer all of them. |
| Revoke a token | A user their own. A cluster admin any. |

A token that is not yours to see is unknown to you: revoking it answers `UNKNOWN_RESOURCE`. A cluster admin's choice of owner is not checked against known users, because users from an identity provider are not stored; a token for a name that nobody holds grants only roles bound to that name.

A session signed in with a token cannot make tokens. A leaked token therefore cannot make successors that outlive it.

Each owner may hold 50 live tokens and the cluster 10,000. Past either, making a token is refused and the message names the limit. The count is of tokens already recorded, so requests made at the same moment can pass a limit by as many as are in flight.

## What a token can do

A connection signed in with a token acts as the token's owner, with the roles bound to the owner's own name. The owner's IdP groups do not count, as for a certificate. A user whose roles all come from IdP groups therefore gets a token that grants nothing: bind a role to the user's name, or make the token for a service account that has bindings of its own ([Authentication and roles](authentication-and-roles.md#bindings)). A token cannot be narrowed to fewer roles than its owner has.

The session ends:

- when the token expires;
- at once when the token is revoked, on every node;
- when the owner's sessions are revoked ([Revoking sessions](authentication-and-roles.md#revoking-sessions)): a token made before the revocation is refused, a token made after it works;
- when the token is removed for any other reason.

`DescribeSessions` lists a token's session with its mechanism and the token's id, never the secret.

## Make, list and revoke

Through a client. The Rust client:

```rust,ignore
let made = client.create_token("ci-pipeline", None, Lifetime::Month).await?;
println!("{}", made.text);   // shown once; store it now

for t in client.describe_tokens(None).await? {
    println!("{:016x} {} {}", t.id, t.name, t.owner);
}
client.revoke_token(made.id).await?;
```

See [Rust client](../developer/rust-client.md#access-tokens). A cluster admin passes `Some("svc-ci")` as the owner to make one for another principal. The console's *Tokens* page is coming.

## Check the cluster

Each node reports, at `/metrics` ([Monitoring](monitoring.md)):

| Metric | Meaning |
| --- | --- |
| `lotse_tokens` | Tokens that have not expired, as this node has applied them. |
| `lotse_token_sign_ins_total{result}` | Sign-ins with a token on this node, `result` `ok` or `failed`. |

A rise in `failed` is a wrong, expired or revoked token; the node says only `authentication failed`, whatever the reason.

## Cut off a user

To cut off one token, revoke it. To cut off every connection of a user, including those signed in with tokens made before now, revoke the user's sessions ([Revoking sessions](authentication-and-roles.md#revoking-sessions)): their older tokens are refused from then on, and a token made afterwards works. The user's tokens stay in the list until they expire or are revoked.
