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

A cluster admin can also revoke one client certificate without the CA key and without copying a file: the console's *Users and access* page (tab *Certificates*) or `lotse-client`'s `revoke_certificate` send `RevokeCertificate` with the issuer (the SHA-256 of the issuer's name in DER form), the serial and the time the certificate expires ([ADR 0090](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0090-revoking-client-certificates-in-the-metadata-group.md)). Every node applies it at once, ends the connections that showed the certificate and refuses it until it expires. The CRL file stays for revoking offline and for node certificates, which this request does not cover.

A revoked certificate does not end a user's other ways in. To cut off a user's sessions, see [Revoking sessions](authentication-and-roles.md#revoking-sessions).
