# 20. Console user groups, role mapping and audit

- Status: Accepted
- Date: 2026-10-27

## Context

Operators, tenants, support staff and auditors need different rights; production data needs care.

## Decision

IdP groups map to broker roles: platform admins, operators, observers and auditors cluster-wide; admin, developer and support per tenant. Seeing metadata and reading message contents are separate permissions; in production only tenant support and admin read contents, and every read is audited. Destructive actions need typing the resource name in production. Break-glass: an operator may elevate to admin for at most 60 minutes with a reason, audited and shown on the overview. The group-to-role mapping lives in replicated cluster metadata, changed through an admin API; `lotse-cli cluster init` creates the first admin group. The audit log is the internal stream `__audit`, hash-chained, forwarded to syslog/SIEM.

## Consequences
- The same rights apply in the console and the CLI.
- Every privileged action leaves a tamper-evident trace.

See also: [design document](../design.md#benutzergruppen).
