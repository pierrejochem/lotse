# Certificates

Every connection speaks TLS 1.3 with certificates from the cluster's CA. Node links need a node certificate on both ends; clients may show a certificate that names their principal ([ADR 0037](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0037-tls-and-node-identity.md)).

## Issue

```sh
lotse certs --config cluster.toml
lotse certs --config cluster.toml --client alice --out ./alice
```

The first command makes, at the paths `[tls]` names:

- a CA: `ca.pem`, with its key beside it as `ca.key`, valid for 3650 days;
- for each node, a certificate and key valid for 365 days. The certificate carries `lotse://node/<id>` and the hosts of the node's addresses;
- an empty revocation list, if `[tls] crl` names a file that does not exist.

Files that exist are kept, so the command can run again after a node is added to the file. Keys are readable by their owner only. `--days <n>` sets another length for new node and client certificates.

The second command makes a client certificate whose common name is the principal: `alice/alice.pem` and `alice/alice.key`. It never overwrites existing files. This is how the first admin signs in; the name must be listed in `auth.admins`.

Keep `ca.key` off the nodes. It signs every certificate of the cluster. A larger setup can bring certificates from its own CA to the same paths instead.

## Check expiry

```sh
lotse check --config cluster.toml
```

warns about the CA and node certificates on this machine that expire within 30 days or already have. Files that are not on this machine are skipped.

## Renew

Remove the node's certificate and key, then run `lotse certs` again:

```sh
rm /etc/lotse/node-2.pem /etc/lotse/node-2.key
lotse certs --config cluster.toml
```

A running node reads its CA, certificate, key and revocation list every 10 s. If any of them changed and the files make a working configuration, every handshake from then on uses them. No restart is needed ([ADR 0060](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0060-certificate-renewal-and-revocation.md)).

If the new files do not load (half written, a key that does not match the certificate, a list that does not parse), the node logs "TLS files do not load; keeping the material in use" once and goes on with what it has.

New files must reach the node's machine yourself; `lotse certs` writes where `[tls]` says.

## Expiry ends connections

A connection ends when the certificate its other end showed expires. Both ends do this. The client or node then reconnects; with renewed files in place the next handshake succeeds, without them it fails ([ADR 0068](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0068-connections-end-when-their-certificate-expires.md)). Clients that sign in with SCRAM or a token show no certificate and are not affected.

## Revoke

```sh
lotse certs --config cluster.toml --revoke alice/alice.pem
```

adds the certificate to the revocation list `[tls] crl` names. The command keeps the entries already there, raises the list's number, and refuses a certificate the cluster's CA did not issue. `--revoke` goes alone, without `--client` or `--days`.

Then copy the list to every node. Within 10 s each node:

- refuses the revoked certificate in every handshake, in both directions and on both link kinds;
- ends every open connection whose other end showed that certificate.

Other connections stay open. Revocation is a file; there is no OCSP. Distributing the file to the nodes is up to you.

### Revoke from the console or a client

A cluster admin can also revoke one client certificate without the CA key and without copying a file: the console's *Users and access* page (tab *Certificates*) or `lotse-client`'s `revoke_certificate` send `RevokeCertificate` with the issuer (the SHA-256 of the issuer's DER name and authority key identifier, so that two CAs with one name are told apart), the serial and the time the certificate expires ([ADR 0090](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0090-revoking-client-certificates-in-the-metadata-group.md)). Every node applies it at once, ends the connections that showed the certificate and refuses it until it expires. The CRL file stays for revoking offline and for node certificates, which this request does not cover.

### See which certificates are in use

Every node reports the client certificates it accepts, and the cluster keeps them for 30 days after they expire. The console's *Certificates* tab lists them, by expiry, with the principal each names, when a node last accepted it (at most an hour late) and its state: valid, expiring within 30 days, expired or revoked. A cluster admin can revoke a valid or expiring one from the list. `lotse-client`'s `describe_certificates` returns the same list ([ADR 0091](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0091-client-certificate-inventory.md)). Certificates revoked only in the CRL file and node certificates are not listed.

### Ask for a client certificate

In the console's *Certificates* tab, *Request certificate* asks the CA holder for a client certificate for a principal. Your browser makes an ECDSA P-256 key and a signing request; the private key is saved as `<principal>.key` once and goes nowhere else, and the console and the cluster hold only the request. The request waits for 7 days.

A cluster admin asks for any principal. Any other signed-in user asks only in their own name and sees only their own requests; asking again replaces their request that is still waiting, so each has at most one. Users' own requests take at most 192 of the 256 places, which leaves room for admins. A certificate signs in without the identity provider's groups, so it grants only the roles bound to the user's name itself; the console says so when none is ([ADR 0119](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0119-users-request-certificates-in-their-own-name.md)).

Every node reports how many requests wait as `lotse_certificate_requests_pending` on its metrics endpoint, and the *Certificates* tab shows the number to cluster admins. An alert on it tells the CA holder when to sign (see [Monitoring](monitoring.md)).

The CA holder, on the machine with the CA key, signs what is waiting:

```sh
lotse certs --config cluster.toml --sign --user admin       # password in LOTSE_PASSWORD, or --cert and --key
```

The command shows each request's principal, who asked and the fingerprint of the key, and signs those you confirm. `--request <id>` (the id or its first digits) with `--yes` signs that one without asking, never all of them; `--out <dir>` also writes each certificate as `<principal>.pem`; `--days` sets the validity (default 365). The certificate is made from the key and the principal only: whatever else a request asks for is ignored. The signed certificate is recorded in the cluster, which takes it only if it has the request's key and principal and is signed by a CA certificate in the nodes' `[tls] ca` file, and the console's list offers it for download; it works with the key file saved earlier ([ADR 0092](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0092-requesting-and-signing-client-certificates.md)).

## Renew a client certificate over its connection

A client certificate renews itself, without the CA holder, once the cluster has a renewal CA ([ADR 0123](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0123-client-certificates-are-renewed-over-a-connection-made-with-them.md)):

```sh
lotse certs --config cluster.toml --renewal-ca      # on the machine with ca.key
```

makes, under the cluster's CA, an intermediate CA that may sign client certificates only: `renewal-ca.pem` and its key `renewal-ca.key` at the path `[tls] renewal_ca` names. Add `renewal_ca` to the cluster file and copy both files to every node. Each node takes them up within 10 s. Keep `ca.key` offline as before.

A certificate may then be renewed from the earlier of its last 30 days and two thirds of its validity, until it expires; a year-long certificate opens on day 243. The client sends a signing request for a new key over a connection made with the certificate. The metadata leader signs a certificate for the same principal, valid for 90 days, and records it in the cluster. The old certificate keeps working until it expires.

```sh
lotse certs --config cluster.toml --renew --cert alice/alice.pem --key alice/alice.key
lotse certs --config cluster.toml --renew --if-due --cert alice/alice.pem --key alice/alice.key   # from a daily timer
```

renews the certificate in `--cert` and writes the new key, then the new certificate (with the renewal CA's after it), over the files. With `--if-due` it does nothing before the window opens. The clients for Java, Kotlin, C# and Node.js can renew by themselves, and every client has a call for it (see the client guides).

A certificate from the renewal CA signs in only while the cluster holds its record for that principal. A copy of the renewal CA's key therefore mints nothing that signs in. Renewing the same certificate a second time replaces the first renewal, whose connections end. The *Certificates* tab lists renewed certificates like any other.

A revoked certificate does not end a user's other ways in. To cut off a user's sessions, see [Revoking sessions](authentication-and-roles.md#revoking-sessions).
