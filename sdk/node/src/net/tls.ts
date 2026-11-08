import { readFile } from "node:fs/promises";

/** The only protocol version. */
export const TLS_PROTOCOL = "TLSv1.3";

/**
 * TLS for client connections: the cluster's CA, and optionally a client certificate.
 *
 * Only TLS 1.3 is spoken, and the node's certificate must be valid for the host the client dialled (a name in its
 * DNS entries, or an IP address in its IP entries). Only the CA given here is trusted, not the roots Node.js ships
 * with. The client key is a PKCS#8 PEM, as `lotse certs` writes it.
 */
export class TlsConfig {
  /** @internal */
  readonly ca: string;
  /** @internal */
  readonly cert: string | undefined;
  /** @internal */
  readonly key: string | undefined;

  private constructor(ca: string, cert: string | undefined, key: string | undefined) {
    this.ca = ca;
    this.cert = cert;
    this.key = key;
  }

  /** Whether a client certificate is presented when a node asks for one. */
  get hasClientCertificate(): boolean {
    return this.key !== undefined;
  }

  /**
   * Trusts the CA certificates in `ca` (PEM text); presents `cert` (the chain, leaf first) with `key` as the client's
   * identity when both are given.
   *
   * @throws RangeError if there is no CA (a node's certificate cannot be checked against nothing), or only one of
   *   `cert` and `key`
   */
  static fromPem(pem: { ca: string; cert?: string; key?: string }): TlsConfig {
    if (!pem.ca.includes("-----BEGIN CERTIFICATE-----")) {
      throw new RangeError("no CA certificate: give the cluster's ca.pem");
    }
    if ((pem.cert === undefined) !== (pem.key === undefined)) {
      throw new RangeError("a client identity needs both the certificate and the key");
    }
    return new TlsConfig(pem.ca, pem.cert, pem.key);
  }

  /** Reads the PEM files: the CA, and optionally the client certificate and its key. */
  static async fromFiles(caPem: string, certPem?: string, keyPem?: string): Promise<TlsConfig> {
    const [ca, cert, key] = await Promise.all([
      readFile(caPem, "utf8"),
      certPem === undefined ? undefined : readFile(certPem, "utf8"),
      keyPem === undefined ? undefined : readFile(keyPem, "utf8"),
    ]);
    return TlsConfig.fromPem({ ca, ...(cert === undefined ? {} : { cert }), ...(key === undefined ? {} : { key }) });
  }
}
