# 91. Client certificate inventory

- Status: Accepted
- Date: 2026-11-09

## Context

[ADR 0090](0090-revoking-client-certificates-in-the-metadata-group.md) lets a cluster admin revoke a client certificate, but only by typing its issuer fingerprint, serial and expiry. The cluster knew nothing of the certificates it accepted beyond what a handshake showed, so an admin could not see which certificates were in use, which were about to expire, or which were revoked.

## Decision

**Nodes report what they accept.** A connection whose certificate authenticates it (`mtls` on, not a node certificate) notes the certificate when it starts and again every ten minutes while it stays open. Every 20 ticks the node's main loop sends what it noted to the metadata leader with the internal request `RecordCertificates` (key 54), through the node's own client, as `SettleVoters` travels. Only broker nodes may send it. A node leaves out what the state it applied already holds with a last sight within the hour; the leader checks again and answers without a write when nothing is new, so reconnecting clients cost the metadata group nothing. One report holds at most 256 certificates.

**State in the metadata group.** A change (tag 21) merges reports into `certs`, keyed by issuer and serial: the principal, validity, the earliest first sight and the latest last sight. Reports from two nodes in either order give the same entry. An entry stays 30 days after its certificate ended and is dropped when a later report applies, by the time the proposing leader carried in the change, so every replica forgets the same ones. The entries are the optional section 8 of the snapshot.

**The list.** `DescribeCertificates` (key 53, cluster admin only, like `DescribeUsers`) answers from the leader's applied state: every inventory entry and every revocation of [ADR 0090](0090-revoking-client-certificates-in-the-metadata-group.md), by expiry, each with its principal, validity, first and last sight and a state: valid, expiring (ending within 30 days), expired or revoked, with the reason and time of a revocation. A certificate revoked before any node saw it is listed with no principal. The states are computed against the leader's clock, which the answer carries.

**What the list does not hold.** A certificate revoked only in the CRL file is not listed, and node certificates are not recorded: the file is the only place that knows them. "Last seen" is at most an hour behind for a connection that stays open, and the cluster records it at most hourly per certificate.

**Console.** `GET /clusters/{cluster}/certificates`, and the *Certificates* tab shows the list with a filter (all, expiring, revoked, expired) and Revoke on valid and expiring rows, the serial typed again. The form of [ADR 0090](0090-revoking-client-certificates-in-the-metadata-group.md) stays for a certificate no node has seen.

## Consequences

- An admin sees which client certificates are in use and about to end, and revokes one from the list, without typing a fingerprint.
- The metadata group gets a write per new certificate and per certificate and hour at most; the state is bounded by the certificates issued and still within the keep time.
- Nothing is recorded when authentication does not need a certificate (`mtls` off) or when no authentication is required: there is no certificate to rely on.
- The inventory is a view of what nodes saw. It is not authoritative about what the CA issued; issuing is the next phase.
- Tests:
  - the state: merging in either order, one entry per issuer and serial, forgetting the long expired, the snapshot round trip;
  - a simulation with certificates: connections of three certificates appear with their principals and states in order of expiry; five new connections with one certificate leave one entry with its first sight; a revocation shows with its reason and an unseen certificate revoked is listed; only a cluster admin lists and only a node reports;
  - the console against a real node that requires authentication, and the demo cluster;
  - the codecs of the Rust, Java, Kotlin, C# and Node.js clients against the shared vectors.
- Mutation checks, each broken on purpose until a test failed:
  - letting a client report certificates;
  - not noting a certificate when its connection starts;
  - not computing the expiring state;
  - taking the latest first sight instead of the earliest;
  - never forgetting expired entries;
  - leaving the inventory out of the snapshot;
  - letting anyone list the certificates;
  - ignoring revocations in the list's states.
- Not mutation-checked: the leader's and the node's skip of reports that add nothing, which changes the number of writes and not the state.
