# 43. A cluster-wide viewer role

- Status: Accepted
- Date: 2026-10-29

## Context

Since ADR 0041 the console's metrics sampler records the history of every stream and queue it may see, and each user's chart sums what that user may see. ADR 0039's roles gave the sampler no way to see everything without being a cluster admin, which ADR 0019 rules out: an admin had to bind it a viewer role in every tenant, again for every new tenant, and a tenant forgotten silently had no history. The cluster's own groups and the commit latency were visible to cluster admins only.

## Decision

**A sixth role, `cluster-viewer`** (maintainer's choice). It permits seeing (`Describe`) and nothing else, in every tenant: every resource, every tenant, and what belongs to the whole cluster (its own groups in `DescribeMetrics`, and in the console the commit latency). It reads no messages, writes nothing, creates and changes nothing, and sees neither users nor role bindings beyond its own (`WhoAmI`). Like `cluster-admin` it is cluster-wide: a binding of it is stored without tenant and prefix, whatever the request names, and deleting a tenant does not remove it. Its wire code is 5.

The console's sampler is meant to hold this role: one binding covers all tenants, also those created later.

## Consequences

- One binding, `console-metrics` → `cluster-viewer`, replaces a viewer binding per tenant.
- Whoever holds the role sees the names, sizes and rates of every tenant's resources. That is less than a cluster admin, but more than a tenant's own viewer.
- The access simulation binds and removes cluster viewers among the other roles, with its own idea of what they permit, and a deterministic test checks what a cluster viewer sees and that it can neither read, write, create, delete, administer tenants, users or bindings, nor promote itself. Each rule was broken on purpose and a test failed, except one equivalent: keeping cluster-wide bindings when a tenant is deleted needs no check of its own, since they name no tenant.
