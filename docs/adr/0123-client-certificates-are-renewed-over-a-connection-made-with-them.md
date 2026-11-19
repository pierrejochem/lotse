# 123. Client certificates are renewed over a connection made with them

- Status: Accepted
- Date: 2026-11-19
- Changes [ADR 0119](0119-users-request-certificates-in-their-own-name.md) and [ADR 0092](0092-requesting-and-signing-client-certificates.md) in part: the cluster now signs one kind of certificate itself

## Context

A client certificate lasts a year. Before ADR 0123, renewing it meant a new request in the console (ADR 0119) or a new certificate from the CA holder: a person signed every one, offline, with the CA key (ADR 0092). The README listed renewing a certificate over a connection made with it as a gap. A service that signs in with a certificate had no way to keep itself running past its certificate's year without a person.

The certificate a connection showed already proves who the client is. A renewal names the same principal for a new key. It grants nothing the old certificate did not.

## Decision

**A connection made with a client certificate in its renewal window may ask for a new one. The metadata leader signs it with an intermediate renewal CA the nodes hold, records it in the metadata group, and the nodes accept a certificate from that CA only while it is recorded** (maintainer's choices: renewal over the connection; an online CA in the cluster; a separate intermediate, recorded only; the last 30 days or two thirds, 90 days; a new request plus a call in every client).

- **The renewal CA.** `lotse certs --renewal-ca` makes an intermediate CA under the cluster's CA, at `[tls] renewal_ca`, with its key at `[tls] renewal_ca_key` (by default the same path ending in `.key`).
  - It is a CA with path length 0, may sign client certificates only (extended key usage clientAuth), and has a P-256 key.
  - The operator copies both files to every node. A node takes them up, or a change to them, within 10 s, like its other TLS files. A key that does not match the certificate, or a CA that the cluster's CA did not sign, is refused.
  - Without these settings a cluster renews nothing, as before.
  - The root CA's key stays offline.
- **When.** A certificate may be renewed from the earlier of its last 30 days and two thirds of its validity, until it expires. A year-long certificate opens on day 243.
- **The request.** `RenewCertificate` (key 60) carries a PKCS #10 signing request, DER, at most 4,096 bytes, signed with the new key. Its subject and extensions are ignored. The answer has the error and its reason, the chain (the new certificate, then the renewal CA's), its end, and the metadata leader as the tagged hint.
- **Who may.** Only a connection that showed a client certificate, while it still acts as that certificate's principal. A connection that signed in again with SASL as someone else is refused with `UNAUTHORIZED`. Any user principal may ask. Nodes, anonymous connections and token sessions have nothing to renew.
- **Signing.** The metadata leader:
  - checks the window against its clock;
  - verifies the request's signature;
  - makes a certificate with the request's key, the principal as its common name, extended key usage clientAuth, no CA rights, and a 16-byte serial from the node's random seam;
  - makes it valid from a minute before now for 90 days, but never past the renewal CA's own end;
  - signs with deterministic ECDSA (RFC 6979), so no randomness is needed beyond the serial.
- **Recording.** The leader proposes `RenewCertificate` (metadata change tag 29) with the new certificate's issuer and serial, the principal, its validity, and the certificate it renews. It answers once the change commits.
  - The state keeps the renewed certificates still valid (snapshot section 17) and lists each in the inventory (ADR 0091).
  - A later renewal of the same certificate replaces the earlier one, whose connections end. A client that retries after a lost answer therefore keeps only the certificate it was last given. If the answer it waits for was replaced meanwhile, it gets `INVALID_REQUEST`.
  - At most 4,096 renewed certificates still valid are kept at once; a renewal that replaces one always fits.
  - Expired ones are dropped when the next renewal applies.
- **Recorded only.** A node closes every client session whose certificate the renewal CA issued but the metadata state does not hold for that principal, as it does for a revoked certificate (ADR 0090).
  - A copy of the renewal CA's key therefore mints nothing that signs in: a certificate the cluster did not record is refused.
  - A node identity counts only when the cluster's CA signed the node certificate itself. A node certificate with any other CA certificate after it in the chain is no node, so the renewal CA cannot make one.
- **The old certificate.** It is not revoked. It keeps working until it expires, so clients can switch to the new files when they next connect.
- **Clients.**
  - Every client has a call that makes a new P-256 key, renews, and returns the key and chain. A second form writes them over the certificate and key files: the key first, each through a file beside it moved into place whole.
  - The Java, Kotlin, C# and Node.js clients can renew by themselves. Given the files, the client looks at the certificate at start and every hour (configurable), and renews once the window opens. A failure is tried again a minute later and is reported by the client.
  - TLS configured from files reads them again for the next connection once either file changed. It takes the pair only when the key matches the certificate, so a connection made between the two writes keeps the old pair.
  - The Rust client has `renew_certificate_if_due`, and `lotse certs --renew [--if-due] --cert <file> --key <file>` renews from the command line. With `--if-due` it does nothing before the window, for a timer to run daily.

Rejected options:

- **Renewal by a request the CA holder signs**, as for a new certificate (ADR 0092, 0119). It keeps every signature offline, but a service still needs a person once a year.
- **The cluster's root key on the nodes.** One stolen node key would then mint any certificate, node certificates included.
- **An intermediate whose certificates count without a record.** It is simpler, but a stolen intermediate key would sign in as anyone until the intermediate is replaced on every node.
- **Revoking the old certificate at once.** A client that has not yet written its new files, or a second process with the same files, would lose its connections.
- **A longer renewed validity, such as a year.** Renewal is automatic, so a short life costs nothing and limits what a lost key is worth.
- **Clients that only return the new files.** Each application would write its own file handling and reloading, and most would get the half-written case wrong.

## Consequences

- A service with a certificate keeps itself signed in: it renews about every two months, and nobody signs anything.
- The renewal CA's key lives on every node. Losing it allows nothing by itself, since unrecorded certificates are refused. With a metadata leader's cooperation it allows renewals, which need a valid certificate of the principal in its window anyway.
- A renewed certificate is seen by a node once the record reaches it. A connection made with it before then is closed, and the client connects again.
- The first renewal of a year-long certificate comes on day 243, so a year of validity effectively becomes about eight months.
- New in the protocol: request key 60, and in the golden vectors a request and two responses. The SDKs encode and decode it.
- The workspace takes the `p256` crate as a direct dependency, already in the tree through the JWT library, so that the leader signs without OS randomness.
- Tests:
  - Unit tests cover the window, a signed certificate's properties, the cap at the renewal CA's end, bad requests, keys and CAs, and the metadata change: round trip, replacement, expiry, snapshot section, inventory and the cap.
  - A simulation renews over a connection, then checks:
    - a certificate outside its window, or a connection without one, is refused;
    - a certificate connection that signed in as someone else is refused;
    - the new certificate signs in on every node, as its principal only;
    - an unrecorded certificate from the renewal CA does not sign in;
    - a second renewal ends the first's connections.
  - On real TLS, `lotse certs --renewal-ca` and `--renew` renew a certificate, and the test then checks:
    - `--if-due` leaves one outside its window alone;
    - the new files sign in and the inventory lists them;
    - a forged certificate from the renewal CA is refused;
    - a second renewal replaces the first.
  - A TLS test checks that a node certificate under an intermediate is no node.
  - Each SDK renews against a real node: the files are written, the next connection shows the new certificate (refused a second renewal, being outside its window), and a client set to renew by itself does.
- Mutation-checked:
  - the recorded-only rule;
  - replacement;
  - the window;
  - the principal check;
  - the cap counting expired certificates, and a replacement refused at the cap;
  - the node identity under an intermediate;
  - in the SDKs, the reload of changed files and the client's window.
