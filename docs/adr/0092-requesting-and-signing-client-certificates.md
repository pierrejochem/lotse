# 92. Requesting and signing client certificates

- Status: Accepted; in part superseded by [ADR 0096](0096-recorded-certificates-are-checked.md) (the broker parses and checks a recorded certificate)
- Date: 2026-11-09

## Context

[ADR 0090](0090-revoking-client-certificates-in-the-metadata-group.md) and [ADR 0091](0091-client-certificate-inventory.md) let a cluster admin revoke and list client certificates in the console. Issuing stayed with `lotse certs --client` on the machine with `ca.key`, which makes the key pair itself and writes the private key to that machine's disk, to be carried to whoever needs it.

The plan wants issuing from the console without a terminal for the admin and without the CA key anywhere a running process can reach it: whoever holds the console or a node must not be able to mint identities ([ADR 0019](0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md)).

## Decision

**Signed offline by the CA holder.** The CA key stays where it is. The console asks, the cluster records the request, and `lotse certs --sign`, run by the CA holder, signs.

**The requester's key never travels.** The browser makes an ECDSA P-256 key pair with WebCrypto and a PKCS#10 signing request signed with that key (WebCrypto cannot write one, so the few DER pieces are in `console/web/src/certificateRequest.ts`). The private key is offered once as a PKCS#8 file; the console and the cluster hold only the request.

**Three requests, all cluster admin:**

- `RequestCertificate` (key 55): a principal and the request in DER form, at most 4096 bytes. The principal is what `lotse certs --client` accepts for a file name: letters, digits, `.`, `_`, `-`, `@`, at most 128 bytes, no leading dot. The id is the SHA-256 of the request, so the same request twice is one. At most 256 requests are held at once.
- `DescribeCertificateRequests` (key 56): the requests, newest first, each with principal, request, who asked, when, when it lapses, a state (pending, signed, expired) and, once signed, the certificate and the time.
- `RecordCertificate` (key 57): sent by `lotse certs --sign` after signing: the request's id, the certificate, and beside it the issuer fingerprint, serial and validity. The broker accepts it only for an open request: one that exists, is unsigned and has not lapsed. It does not parse the certificate, which brings no X.509 code into the broker; the facts are the CA holder's word, and a certificate is only ever as good as its signature at the handshake. A request asked for by a user for their own name is not built: asking is a cluster admin's act.

**State in the metadata group.** Changes with tags 22 and 23 keep `cert_requests` by id. A request lapses 7 days after it was asked; a signed one stays 30 days after signing, for the requester to fetch the certificate. Both are dropped when a later change applies, by the time the proposing leader carried in it, so every replica forgets the same ones. The entries are the optional section 9 of the snapshot. Recording a certificate also enters it into the inventory of [ADR 0091](0091-client-certificate-inventory.md) with first and last sight 0 ("no node has seen it"): a first sight by a node replaces the 0, where before the earliest sight won, and 0 would always have been earliest.

**What the CA holder signs.** `lotse certs --sign` lists the pending requests, and for each it reads the request, checks that it is signed with the key it names, that the key is ECDSA P-256 and that the name in it, if any, is the principal, shows the principal, who asked and the SHA-256 of the public key (the console shows the same value, so the two sides can compare), and signs what the CA holder confirms. `--yes` needs `--request <id>`, so it never signs everything. The certificate is made from the key and the principal only: whatever the request asks for besides (extensions, alternative names) is ignored, and the certificate is valid for client authentication alone. The command records the certificate in the cluster and, with `--out`, writes `<principal>.pem`. It connects as an admin by the same flags as `lotse node`.

**Console.** `GET` and `POST /clusters/{cluster}/certificate-requests`; the *Certificates* tab lists requests with their state and a download for signed certificates, and *Request certificate* makes the key and the request, sends the request and saves the key at once.

## Consequences

- An admin gets a certificate for a service without a terminal and without the private key crossing the network or resting on the CA holder's machine; the CA holder confirms with one command, and on a small cluster, where both are one person, one step.
- Issuing waits for the CA holder; a request that nobody signs lapses after seven days.
- The cluster's records of what was signed are the CA holder's word: a cluster admin who records a false certificate fills the list and the request, and nothing else, since the handshake checks the signature.
- A requester who loses the key file asks again: the console cannot give the key.
- Requests are bounded to 256 and 4 KiB each; certificates to 8 KiB.
- Tests:
  - the state: a repeat request is one, an answer is taken once, requests lapse and signed ones go after 30 days, the snapshot round trip, and a node's first sight replacing 0;
  - a simulation: bad names and sizes refused, the cap of 256, only cluster admins ask, list or record, an answer needs an open request and a sound record, a signed request carries the certificate and enters the inventory;
  - `lotse certs --sign` against a running node: nothing signed without confirmation, `--yes` only with `--request`, the signed certificate in the cluster equal to the file written, in the inventory;
  - signing: the certificate has the requester's key, no CA flag, no alternative names, client authentication only, and the cluster CA as issuer; refused: another principal, a damaged signature, junk, another key type;
  - a request the browser code made (checked with `openssl req -verify`) is read and signed by the Rust side, and both sides compute the same key fingerprint;
  - the console against the demo cluster and a real node that requires authentication; Vitest for the DER encoding;
  - the codecs of the Rust, Java, Kotlin, C# and Node.js clients against the shared vectors.
- Mutation checks, each broken on purpose until a test failed:
  - letting a non-admin ask for a certificate;
  - not checking the principal's characters;
  - removing the cap of 256 requests;
  - recording a certificate for a request that is not open;
  - a repeat request overwriting the first;
  - a second answer replacing the first;
  - never forgetting requests;
  - keeping unsigned requests past their seven days;
  - letting 0 ("not seen") win the merge of first sights;
  - leaving a signed certificate out of the inventory;
  - leaving the requests out of the snapshot;
  - signing without checking the name in the request, a key that is not ECDSA P-256, or honouring the extensions a request asks for;
  - `--yes` signing without `--request`, and signing without asking.
- Not mutation-checked: the handler's own check that a request has not lapsed when a certificate is recorded, because applying the change drops a lapsed request first, so the check only saves a write. The codecs of the four SDKs rely on the golden vectors.
