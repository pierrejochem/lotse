# 37. TLS and node identity

- Status: Accepted
- Date: 2026-10-28

## Context

ADR 0004 promised TLS 1.3 on the native protocol, mTLS for services and SASL for users; ADRs 0025 and 0026 shipped plain TCP between nodes and to clients. Any process that reaches a node's port can speak Raft to it or read every stream. Authentication and authorization (the next ADRs) need a channel that cannot be read or altered, and they need to know which connections come from nodes. The runtime seam (ADR 0006) is the only place Lotse touches the network, and the simulation must keep reproducing whatever the real network does.

## Decision

**TLS 1.3 on every connection, with rustls** (maintainer's choice of the full design). `lotse-rt-tokio` speaks TLS 1.3 only, through rustls 0.23 with the ring provider, and no session resumption: connections are long-lived, and unread tickets reset a socket on close. Handshakes run beside the accept loop, each with a 10 s limit, so a slow or hostile peer delays nobody.

**The seam names the link and the peer.** `Net::listen` and `Net::connect` take a `Link`: `Node` for Raft between nodes, `Client` for the native protocol (also when a node is the client, writing transaction markers). Every `Conn` reports a `PeerIdentity`, what the other end proved: `Node(id)` or `Principal(name)`. `Net::verifies_peers` says whether identities are proved at all.

**Node certificates, from the cluster's CA** (maintainer's choice). One CA vouches for everything. A node certificate carries the URI `lotse://node/<id>` (decimal, no leading zeros) among its subject alternative names, and its addresses as DNS or IP names; a certificate that claims two nodes, or misspells one, names nobody. Any other certificate names a principal by its subject's common name.

- A node link needs a node certificate on both ends. The chain is checked against the CA; the host name is not, because nodes dial each other by configured address. Instead, the node checks the id: it reads frames only from a member of its configuration, only that member's own frames, and sends only once the certificate names the node it meant to reach.
- A client link checks the server's chain and host name like any TLS client, and takes a client certificate when one is offered.

**Plain TCP only on loopback, and only when asked for** (maintainer's choice). A runtime given neither TLS (`with_tls`) nor `plaintext_on_loopback` refuses the network. Plain TCP resolves every address it listens on or dials and refuses anything that is not loopback; it logs a warning. Nothing proves identities then, and nodes trust the network.

**The simulation verifies too.** `SimNet::verify_peers` turns on what TLS does: runtimes carry identities (`SimRuntime::with_identity`), a node link without a node identity on both ends is refused at connect, a client link without one on the server likewise. The node simulation runs half its seeds this way. One difference stays: with TLS 1.3 a client finishes its handshake before the server has judged the client's certificate, so a refused client sees the refusal on its first read; the simulation refuses at connect. Callers handle both, since a connection may break at any time.

**A throwaway CA for tests.** The `dev-certs` feature of `lotse-rt-tokio` makes a self-signed CA and node and principal certificates with rcgen; it is not for production.

## Consequences

- A node of another cluster, a node outside the configuration, or one member speaking for another gets nothing in; an address taken over by someone else gets no frames. Tests on real sockets and in the simulation check this, and each rule is mutation-checked. Requiring a client certificate on node links is enforced twice (by rustls, then by the node-certificate check); breaking either alone is caught by the other, so that pair counts as one rule.
- Everything that used the network must choose: tests, the example cluster and the console's native clusters run plain on loopback, or TLS with `ca_file`.
- Operators need a CA, node certificates with the node URI and the addresses clients dial, and, for services, client certificates. Rotation means restarting a node with new files for now; reloading them is not built yet.
- Certificate revocation is not checked; short-lived certificates are the way to retire one.
- Client links prove the client's identity only with a certificate. Users authenticate with SASL on top (next ADR).
