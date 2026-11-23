# Security policy

Lotse carries other people's data and decides who may read and change it, so a weakness in it matters. Thank you for reporting one privately first.

## Reporting a vulnerability

Report it through GitHub's private vulnerability reporting: [open a draft advisory](https://github.com/pierrejochem/lotse/security/advisories/new). Only the maintainer sees it. Do not open a public issue, pull request or discussion for it.

Please include:

- what an attacker can do, and what they need first (network access to a node, a client certificate, a role, a signed-in console session);
- the release or commit, and how the cluster was set up (the relevant parts of the cluster file, TLS and authentication settings);
- steps or a test that shows it, if you have one.

You get an answer within seven days. Once the weakness is confirmed, it is fixed in a new release, and the advisory is published with it, with credit to you unless you ask otherwise. Please keep it private until then.

## Supported versions

Lotse is at 0.x. Fixes go into the next release from `main`; older releases do not get them. Upgrade one minor release at a time ([ADR 0127](docs/adr/0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)).

## Scope

In scope: the `lotse` binary (the node, `lotse certs`, `lotse mcp`), the management console, the Rust crates, the client SDKs in `sdk/`, and the packages, image and Helm chart in `packaging/`.

Limits that are decided and documented are not vulnerabilities, though a way around one is. They are listed in [docs/status.md](docs/status.md), for example: packages are not signed (`SHA256SUMS` and the reproducible build are the checks), plain TCP is possible only on loopback and only when asked for, and the CRL file is copied to the nodes by the operator.

## How Lotse protects itself

- Every connection speaks TLS 1.3 with certificates from the cluster's CA; node links need node certificates on both ends ([ADR 0037](docs/adr/0037-tls-and-node-identity.md)).
- Clients authenticate with a certificate, SCRAM-SHA-512, OAUTHBEARER or an access token ([ADR 0038](docs/adr/0038-authentication.md), [ADR 0133](docs/adr/0133-access-tokens-with-fixed-lifetimes.md)).
- Every request is checked against tenants and roles; what a principal may not see is unknown to it ([ADR 0039](docs/adr/0039-tenants-and-roles.md)).
- The console and `lotse mcp` act as the signed-in user and hold no rights of their own ([ADR 0019](docs/adr/0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md), [ADR 0134](docs/adr/0134-lotse-mcp-a-separate-command-for-ai-agents.md)).
- Releases are built reproducibly and published only when two independent builds match byte for byte ([ADR 0126](docs/adr/0126-reproducible-releases-built-by-one-script-for-every-init-system.md)).
