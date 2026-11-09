# Authentication and roles

## Ways in

A client signs in in one of three ways ([ADR 0038](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0038-authentication.md)). Each can be switched off in `[auth]`.

| Way | Key | How the principal is named |
| --- | --- | --- |
| Client certificate | `mtls` | The certificate's common name. |
| SASL SCRAM-SHA-512 | `scram` | The user name. Users and passwords are set at runtime. |
| SASL OAUTHBEARER | `[auth.oidc]` | The token's `principal_claim` (default `sub`). |

With `auth.required = true`, the default, a connection may send nothing but `ApiVersions` until it has authenticated. With `required = false`, clients act as anonymous and may do everything; this needs `plaintext_on_loopback` and is for trying Lotse on one machine.

### Certificates

Make a client certificate with `lotse certs --client <principal> --out <dir>`. See [Certificates](certificates.md).

### SCRAM users

Credentials live in the metadata group and are checked by every node from its applied state. The broker stores a salt, an iteration count and two derived keys, never the password; the client derives them, so the password does not leave it either. A cluster admin sets or removes a user through a client (`set_user_password` and `delete_user` in `lotse-client`, `AlterUser` on the wire) and lists users with `DescribeUsers`.

`lotse node` commands can sign in as a SCRAM user: pass `--user <name>` and put the password in `LOTSE_PASSWORD`.

### OIDC tokens

Set `[auth.oidc]` with the `issuer` and the `audience`. A node fetches the issuer's discovery document and key set itself and caches the keys. A token must:

- be signed with an asymmetric algorithm by a key of the set;
- name the configured issuer and audience;
- not be expired (60 s of slack for clocks);
- carry the principal in `principal_claim`.

The groups in `groups_claim` (default `groups`) go with the principal for authorization. A token with an unknown key id makes a node fetch the key set again, at most every 30 s.

### Sessions

A session lasts `max_session_s` (default 3600 s), and a token's session no longer than the token. A client authenticates again on the same connection after four fifths of that time. Authenticating again cannot change the principal. A removed user or an expired token is thus cut off within the session time.

## Tenants

Every resource belongs to a tenant. The tenant `default` always exists. `auth.tenants` names more that exist from the start; a cluster admin creates and deletes others at runtime (`CreateTenant`, `DeleteTenant`). A tenant that holds resources cannot be deleted. Resource names are unique across the cluster ([ADR 0039](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0039-tenants-and-roles.md)).

Transactional ids and consumer group ids live in a tenant too ([ADR 0062](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0062-transactional-and-group-ids-live-in-a-tenant.md)). A tenant deleted and created again starts afresh ([ADR 0069](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0069-tenant-incarnations.md)).

## Roles

Six roles. Four are per tenant; two are cluster-wide.

| Role | May |
| --- | --- |
| `cluster-admin` | Everything, everywhere: users, tenants, role bindings, nodes, pinned leaders, session revocation. |
| `cluster-viewer` | See every resource and tenant and the cluster's own metrics, in every tenant. Reads no messages, writes nothing, sees neither users nor other principals' bindings ([ADR 0043](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0043-cluster-viewer-role.md)). For monitoring. |
| `tenant-admin` | Create, change, delete and bind resources in the tenant, and what the three roles below may. |
| `producer` | Write: produce, publish, transactions. |
| `consumer` | Read: fetch, subscribe, acknowledge, consumer groups and their offsets. |
| `viewer` | See: metadata, settings, metrics. |

A node principal, shown by a node certificate, may look up metadata and write transaction markers, and nothing else.

What a principal may not see is unknown to it: metadata leaves it out, and `DescribeTenants` and `DescribeMetrics` show only what its roles allow. A request that needs a role the principal lacks is refused with `UNAUTHORIZED` and a reason.

### Bindings

A binding gives a role to a user or to a group at the identity provider, in a tenant, for the resources whose names start with a prefix. An empty prefix means all resources of the tenant. The bindings `cluster-admin` and `cluster-viewer` are stored without tenant and prefix.

Bindings are stored in the metadata group. A cluster admin changes them with `AlterRoleBindings` (`alter_role_bindings` in `lotse-client`) and lists them with `DescribeRoleBindings`. A client asks which bindings apply to it with `WhoAmI`. A removed binding takes effect on each node once that node applies the metadata group.

The principals in `auth.admins` are cluster admins from the start. They create the first tenants and bindings. These are configured bindings and cannot be removed at runtime.

## Revoking sessions

A cluster admin revokes the sessions of a user or of an IdP group with `RevokeSessions` (`revoke_sessions` in `lotse-client`) ([ADR 0070](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0070-revoking-sessions-by-principal.md)). The request names the user or group and a time; a time of 0 means now, by the metadata leader's clock.

A session counts as older than the revocation when its credential dates from before it:

- a token's `iat` (a token without one counts as time 0);
- the moment of a SCRAM login;
- the moment a certificate connection was made.

Every node then closes the connections of revoked sessions and refuses logins and tokens that date from before the revocation. The client connects again:

- a SCRAM user signs in with the password and gets a new session;
- a token user needs a token issued after the revocation;
- a certificate user gets in again. To keep a certificate out, revoke it in the CRL ([Certificates](certificates.md#revoke)).

Revocation is per user or group, not per token.

## Where to do it

`lotse node add`, `remove` and `list` are the only admin requests the `lotse` command line sends. Users, tenants, role bindings, settings, pins and session revocation go through a client: `lotse-client`, the Java and Kotlin SDKs, or the console, which manages users, role bindings and session revocation on its *Users and access* page as the signed-in user: the browser derives a password's SCRAM credential, so the console never sees the password ([ADR 0089](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0089-console-users-and-access.md)). The console signs users in with OpenID Connect and acts as the signed-in user ([ADR 0040](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0040-console-sign-in.md)).
