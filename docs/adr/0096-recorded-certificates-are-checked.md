# 96. Recorded certificates are checked; issuers by name and key identifier

- Status: Accepted
- Date: 2026-11-11

## Context
`RecordCertificate` (ADR 0092) stored the certificate the CA holder signed together with its issuer fingerprint, serial and validity as the sender stated them. The broker did not parse the certificate, to keep X.509 code out of it: the inventory showed whatever a cluster admin recorded, and only a handshake ever checked a signature. A wrong or forged record could name any issuer, serial and principal, and a requester could download a certificate that does not fit their key.

Revocations (ADR 0090) and the inventory (ADR 0091) named an issuer by the SHA-256 of its distinguished name. Two CAs with one name were one issuer, and the obvious case is a CA rotation that keeps the name: during the overlap the old and the new CA's certificates were indistinguishable by issuer.

Two decisions, each the maintainer's choice of the recommended option. No release has been made, so no stored fingerprint needs to be carried over.

## Decision
**The broker checks a recorded certificate.** `lotse_auth::x509::check_signed` (pure, `x509-parser` with ring for signatures) is called by the metadata leader for every `RecordCertificate` against the open request it answers. The certificate must be one DER certificate with nothing after it; have the public key of the request's PKCS#10; carry the request's principal as its only common name; be neither a CA (basic constraints) nor a node certificate (a `lotse://node/` URI); end after it starts; and be signed by one of the cluster's CA certificates whose name is the certificate's issuer. The issuer fingerprint, serial and validity in the request must be what the certificate says of itself. Anything else is refused with `INVALID_REQUEST` and a message that says which check failed. The time is not checked: a certificate may start later, and an expired one records nothing harmful.

The CA certificates come from the `[tls] ca` file. `AuthConfig::client_cas` holds them in a handle shared with the reload loop: `lotse run` fills it at start, and every reload that takes up a changed CA bundle (ADR 0060) replaces it, so a node checks against the bundle its handshakes use. A node without TLS has none and refuses every record.

**An issuer is its name and its key identifier.** The issuer fingerprint is the SHA-256 of the issuer's distinguished name in DER form followed by the certificate's authority key identifier, if it has one; without the extension it is the hash of the name alone, as before. A DER name delimits itself, so the two parts cannot run into each other. `lotse certs` writes the extension in every certificate it issues, so two CAs with one name but different keys are two issuers. `lotse_auth::x509::issuer_fingerprint` computes it for the Tokio runtime's `peer_cert` (revocation checks and the inventory reports), for `lotse certs --sign` and for the check above. The protocol is unchanged: the field is still 32 bytes.

## Consequences
The inventory lists only certificates that fit their request and come from the cluster's CA, and a downloaded certificate works with the key the browser saved. `lotse-auth` and with it the broker parse X.509 and verify signatures; the parser is the one the runtime and `lotse certs` already use.

A certificate a CA signed without the authority key identifier still has the name alone as its issuer, so two such CAs with one name remain one issuer; certificates from `lotse certs` always carry it. The revocation list file (ADR 0060) names its issuer by name, as CRLs do, so a list of one CA applies to a certificate of another CA with the same name and the same serial; serials from `lotse certs` are random, so such a match does not happen by accident.

Tested: unit tests for every refusal and for the fingerprint (one CA is one issuer, two CAs with one name are two, without the extension the name alone); a simulation in which a cluster admin records a certificate with real Ed25519 keys (deterministic signatures) and the broker refuses another key, another principal, another CA of the same name, junk and every mismatching fact; `lotse certs --sign` against a running node; a reloaded CA bundle reaching the check. Breaking any one check makes a test fail.
