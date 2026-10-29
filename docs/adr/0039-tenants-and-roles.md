# 39. Tenants and roles

- Status: Accepted
- Date: 2026-10-28

## Context

Since ADR 0038 a node knows who each client is, but every authenticated principal may still do everything. ADRs 0019 and 0021 promised tenants, roles checked by the brokers, and a console that shows each user only what they may see. Who may do what has to be decided the same way on every node, kept as durable as the resources themselves, and changed at runtime without restarting anything.

## Decision

**Tenants are created explicitly** (maintainer's choice). A cluster admin creates and deletes tenants (`CreateTenant` 34, `DeleteTenant` 35); `DescribeTenants` (36) lists those the caller has a role in, with how many resources each holds. A tenant that holds resources cannot be deleted. The tenant `default` always exists; the configuration can name more and assign configured resources to them (otherwise they belong to `default`). Every resource carries its tenant: create requests name it, `Metadata` reports it. Resource names stay unique across the cluster.

**Five roles, bound per tenant, optionally to a name prefix** (maintainer's choice of roles with resource patterns):

| Role | May |
| --- | --- |
| cluster-admin | everything, everywhere: users, tenants, role bindings |
| tenant-admin | create, change, delete and bind resources in the tenant, and what the roles below may |
| producer | write: produce, publish, transactions |
| consumer | read: fetch, subscribe and acknowledge, consumer groups and their offsets |
| viewer | see: metadata, settings, metrics |

A binding gives a role to a user or to a group at the identity provider (from the token's groups claim), in a tenant, for the resources whose names start with its prefix (empty for all). Bindings are stored in the metadata group and changed with `AlterRoleBindings` (37), listed with `DescribeRoleBindings` (38); `WhoAmI` (39) tells a client who it is and which bindings apply. The checks live in `lotse-auth` and depend on nothing but the bindings, so every node and the console decide alike.

**A bootstrap admin from the configuration** (maintainer's choice). `AuthConfig::admins` names principals that are cluster admins from the start; they create the first tenants and bindings. Configured bindings cannot be removed at runtime.

**Every request is checked before it is served**, on the node that receives it, against the bindings it has applied. A request that needs a role the principal lacks is refused as a whole with `UNAUTHORIZED` and a reason (`Response::Refused`, a frame-level error with the reason in a tagged field); clients report it at once instead of retrying. What a principal may not see is unknown to it: `Metadata` leaves it out or reports it as unknown, `DescribeTenants` and `DescribeMetrics` show only what the principal may see (the cluster's own groups only to cluster admins).

**Nodes do only what nodes need.** A node principal (by its certificate) may look up metadata and write transaction markers, nothing else; and only nodes may write transaction markers, so no user can end someone else's transaction. Without authentication required, clients act as anonymous and may do everything.

**Applying stays independent of configuration.** Whether a tenant exists is checked when a create is proposed, on the metadata leader; applying refuses only creates in a tenant deleted since (a tombstone), so a create that raced a tenant's deletion leaves nothing behind. Admin requests check leadership before anything else, since only the leader's applied state holds every change it acknowledged.

## Consequences

- A binding added or removed takes effect on each node once it applies the metadata group: normally within milliseconds, but a node cut off from the metadata group decides on what it last applied. The simulation checks that nothing is allowed that no binding ever allowed, not that a removal is instant.
- Transactional ids and consumer group ids are not scoped to tenants: a principal with write access somewhere can use any transactional id (and fence its producer), one with read access any group id. Tying them to tenants or principals is open.
- Names are unique across tenants, so a name is visible as taken even to a principal who may not see the resource (a create fails with `RESOURCE_EXISTS`).
- A refused request tells the principal which role it lacks for which resource; a resource it may not see at all is reported unknown, as if it did not exist.
- A simulation runs three or five nodes that require authentication while a cluster admin keeps adding and removing bindings and changing passwords and four users create, write, read and list; hosts are partitioned and nodes lose power (maintainer's choice of a simulation of its own). It checks that every request that succeeded was allowed by a binding the admin had asked for before it completed, that listings show only what could be seen, and that after healing every user can do exactly what the bindings left by the admin allow (acknowledged changes are certain, failed ones leave their binding in doubt). Its idea of which role permits what is written out in the test, not taken from the code under test: with the shared one, broken rules went unnoticed. CI runs it in a job of its own, 1,000 seeds per push and 5,000 nightly.
- Deterministic tests check each role, prefix and tenant, groups, revocation, tenant deletion, every request on a resource, markers and nodes. Each rule was broken on purpose and a test failed, except two backed up by another check: a node principal granted every operation in the role check (the request check lets a node do nothing but look up metadata and write markers), and a tenant admin permitted cluster administration (a cluster-wide operation names no tenant, so the tenant scope already rules it out).
- Found on the way: admin requests checked their arguments against the receiving node's state before checking that it leads the metadata group, so a lagging follower refused a tenant that existed instead of pointing to the leader (the code was wrong); and consumer group heartbeats swallowed every error, so a refused member retried forever (the code was wrong).
- Found later by the nightly run (seed 1874): the client kept a refused request's error in a slot shared by all its requests, and the next request that had to try again reported that refusal as its own, for another resource (the code was wrong). The slot now keeps only failed logins, which hold for every request, and is cleared once a login succeeds; a deterministic test checks both. And seed 322: a create whose answer was lost was retried, the retry's login failed (the password had changed), and the client reported a failed login, though the create had been carried out (the code was wrong). A failed login or a refusal after a lost answer is now reported as an unknown outcome.
- The metadata state gains tenants, their tombstones, role bindings and each resource's tenant; the snapshot format becomes 9.
