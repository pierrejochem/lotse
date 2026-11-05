# 60. Certificate renewal and revocation

- Status: Accepted
- Date: 2026-11-05

## Context

A node read its CA, certificate and key once at start, and nothing was checked against a revocation list. A lost client key or a decommissioned machine's node certificate stayed good for as long as it was valid, and `lotse certs` made certificates that were valid until the year 4096. Renewing anything meant restarting every node. The README listed all of this as a known gap.

## Decision

**Revocation is a CRL file** (maintainer's choice). `[tls] crl` names a certificate revocation list in PEM, signed by the cluster's CA. The same file goes to every node, like the CA. Every handshake checks the other end's certificate against it, on both link kinds and in both directions: a node refuses a revoked client or node, and a node dialling out refuses a revoked server. Only end-entity certificates are checked. A certificate whose issuer has no list here counts as not revoked, so a list is optional and a client trusting the CA without one still works. rustls does the check. An OCSP responder was the alternative; it would be one more service to run and reach, for clusters whose certificates come from one CA the operator holds.

**A running node takes up changed files.** Every 10 s it reads its CA, certificate, key and revocation list. If anything changed and the files make a working configuration, every handshake from then on uses them, and every open connection whose other end showed a certificate the new list revokes is ended. Connections the list does not name stay open. If the files do not load (half written, a key that is not the certificate's, a list that does not parse), the node logs it once and keeps the material it has. A mistake in the files never takes a running node off the network. `TokioRuntime::reload_tls` does the swap for a runtime and all its clones; the runtime remembers the peer certificate of each connection for that purpose.

**`lotse certs` makes certificates that end** (maintainer's choice). Node and client certificates are valid for one year, `--days` sets another length, and a new CA is valid for ten years. If `[tls] crl` is set and the file is missing, `lotse certs` writes an empty list, so nodes start. `lotse certs --revoke <cert.pem>` adds a certificate to the list: it keeps the entries already there, raises the list's number, and refuses a certificate the cluster's CA did not issue. The list is written beside the old one and renamed over it, so a node never reads half a list. `lotse check` warns about the CA and node certificates on the machine that expire within 30 days or already have. To renew a node's certificate, remove it and its key and run `lotse certs` again; the node picks up the new files.

## Consequences

- A lost key is shut out within about 10 s of the new list reaching the nodes, without a restart; renewed certificates and a new CA bundle are taken up the same way.
- Copying the list and the certificates to the nodes is the operator's job, as before. A node that has not received the new list still lets the revoked certificate in.
- A revoked node certificate cuts that node off: every other node refuses its links, both ways.
- The list's next-update time (ten years ahead) is not enforced, so an old list never stops a node. A list that is replaced only sometimes still works.
- An open connection whose certificate expires stays open until it closes; only new handshakes check validity. Clients that sign in with SCRAM or a token and show no certificate are not affected by the list; their sessions end as ADR 0038 says.
- A CA rotation needs a bundle with both CAs for a while, which the CA file can already hold.
- Tests:
  - On real sockets: a revoked client and a revoked server are refused in the handshake. A reload, also through a clone of the runtime, ends exactly the revoked connections, including a receive already waiting, and the revoked certificate cannot come back. A reload reaches the next handshake. A list that does not parse and a key that is not its certificate's are refused.
  - With the binary: the validity lengths, the `check` warning, `--revoke` keeping earlier entries, repeats and foreign certificates.
  - A node's reload loop, against a list that does not load and then against a revocation.
- Mutation checks: these were broken on purpose, and a test failed for each:
  - leaving the list out of either verifier;
  - not ending revoked connections, or not marking them;
  - not taking the new material;
  - the reload loop never reloading;
  - `--revoke` dropping earlier entries;
  - certificates without a validity;
  - the CA check on `--revoke`;
  - the empty list on `lotse certs`;
  - the expiry warning.
