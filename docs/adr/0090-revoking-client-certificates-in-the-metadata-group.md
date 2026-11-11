# 90. Revoking client certificates in the metadata group

- Status: Accepted; in part superseded by [ADR 0096](0096-recorded-certificates-are-checked.md) (an issuer is its name and the authority key identifier, not its name alone)
- Date: 2026-11-09

## Context

A client certificate was revoked only through the CRL file ([ADR 0060](0060-certificate-renewal-and-revocation.md)): someone with the CA key adds it with `lotse certs --revoke` and copies the file to every node by hand, and each node takes it up within 10 s. A cluster admin without a shell on the nodes could not cut off a certificate, and the console could only show what the file held. The plan for users and certificates in the console (phase 2) wants a revocation that every node applies at once, as the signed-in cluster admin and without the CA key, like `RevokeSessions` ([ADR 0070](0070-revoking-sessions-by-principal.md)).

## Decision

**A request, `RevokeCertificate` (key 52).** Cluster admin only. It carries the issuer (the SHA-256 of the issuer's name in DER form, 32 bytes), the serial, the moment the certificate expires (`not_after_ms`) and a reason. It is answered with the plain admin answer. The broker refuses a serial of 0 or more than 64 bytes, a reason over 256 bytes, and a certificate that has expired already, which is refused anyway.

**State in the metadata group.** A change (tag 20) puts the entry into `revoked_certs`, keyed by issuer and serial; the first revocation stands and a repeat changes nothing. The proposing leader's clock travels in the change (`at_ms`), and applying it drops every entry whose certificate ended before `at_ms`, so every replica forgets the same ones without a clock of its own. The entries are the optional section 7 of the snapshot.

**The seam.** `lotse_rt::Conn` gains `peer_cert() -> Option<CertInfo>` (issuer fingerprint, serial, validity), `None` without a certificate. The Tokio runtime fills it from the peer certificate of the handshake; the simulation gives a runtime a certificate with `with_cert`. The check lives in the server, not in the TLS layer, so a revoked certificate still completes the handshake: it is the first request that finds the connection closed.

**Enforcement.** Every node publishes the applied set to its `Auth` and wakes the signal that `RevokeSessions` already uses. A session whose connection showed a revoked certificate counts as revoked: the connection ends, also while it waits, and a new one with the certificate ends the same way, whatever the connection then authenticates as. Node certificates are not covered: node links keep the CRL file, so a cluster admin cannot cut a node off by mistake.

**The CRL file stays.** The two sources both apply; the file remains the way to revoke offline and to revoke node certificates, and the handshake check of ADR 0060 is unchanged.

**The issuer is its name.** As in a CRL, an issuer is identified by its distinguished name. Two issuers with the same name are one issuer here; a cluster has one CA.

**Console.** `POST /clusters/{cluster}/certificates/revoke` acts as the signed-in user and asks for the serial typed again, in the API too. Until the inventory exists (the next ADR), the *Certificates* tab takes issuer, serial and expiry typed in.

## Consequences

- A cluster admin revokes a client certificate in every node within one apply, without the CA key or a shell.
- Revocations stay in the state until the certificate expires, and are dropped by the next revocation after that; the state is bounded by the certificates revoked and still valid.
- A client must know the issuer fingerprint, serial and expiry. The inventory (phase 2b) removes that.
- Tests:
  - the state: repeats, the issuer as part of the key, forgetting expired entries, a certificate that has expired already, the snapshot round trip;
  - a simulation with certificates: two certificates of one principal and one of another issuer with the same serial; only a cluster admin revokes; the revoked one's connection ends, a new connection with it ends, the others go on;
  - nodes may not revoke, in the access test;
  - real TLS: the connection reports issuer, serial and validity of the certificate it showed;
  - the console against a real node that requires authentication, and the demo cluster;
  - the codecs of the Rust, Java, Kotlin, C# and Node.js clients against the shared vectors.
- Mutation checks, each broken on purpose until a test failed:
  - leaving the revoked-certificate check out of the session;
  - not waking connections when the set changes;
  - dropping the cluster-admin check;
  - keying by serial only;
  - not forgetting expired entries;
  - leaving the entries out of the snapshot.
