import { X509Certificate, createPrivateKey } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { readFile } from "node:fs/promises";

/** The only protocol version. */
export const TLS_PROTOCOL = "TLSv1.3";

/**
 * TLS for client connections: the cluster's CA, and optionally a client certificate.
 *
 * Only TLS 1.3 is spoken, and the node's certificate must be valid for the host the client dialled (a name in its
 * DNS entries, or an IP address in its IP entries). Only the CA given here is trusted, not the roots Node.js ships
 * with. The client key is a PKCS#8 PEM, as `lotse certs` writes it.
 *
 * A client identity read with `fromFiles` is read again for the next connection once either file changed, so a
 * renewed certificate (ADR 0123) is used without a restart. If the files cannot be read then, half written for
 * instance, or the key does not match the certificate yet, the identity read before is kept.
 */
export class TlsConfig {
  /** @internal */
  readonly ca: string;
  private cert: string | undefined;
  private key: string | undefined;
  /** The identity's files and their modification times when last read, if it came from files. */
  private files: { cert: string; key: string; stamp: string } | undefined;

  private constructor(ca: string, cert: string | undefined, key: string | undefined) {
    this.ca = ca;
    this.cert = cert;
    this.key = key;
  }

  /**
   * The identity to present: read again first if its files changed.
   *
   * @internal
   */
  identity(): { cert?: string; key?: string } {
    const f = this.files;
    if (f !== undefined) {
      const stamp = TlsConfig.stamp(f.cert, f.key);
      if (stamp !== undefined && stamp !== f.stamp) {
        try {
          const cert = readFileSync(f.cert, "utf8");
          const key = readFileSync(f.key, "utf8");
          // Between the writes of a renewal the key may be new and the certificate not yet.
          if (new X509Certificate(cert).checkPrivateKey(createPrivateKey(key))) {
            this.cert = cert;
            this.key = key;
            f.stamp = stamp;
          }
        } catch {
          // half written or unreadable: keep what worked, look again next time
        }
      }
    }
    return {
      ...(this.cert === undefined ? {} : { cert: this.cert }),
      ...(this.key === undefined ? {} : { key: this.key }),
    };
  }

  /** Both files' modification times, or undefined if either cannot be read. */
  private static stamp(cert: string, key: string): string | undefined {
    try {
      return `${statSync(cert).mtimeMs}/${statSync(key).mtimeMs}`;
    } catch {
      return undefined;
    }
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
    const stamp = certPem === undefined || keyPem === undefined ? undefined : TlsConfig.stamp(certPem, keyPem);
    const [ca, cert, key] = await Promise.all([
      readFile(caPem, "utf8"),
      certPem === undefined ? undefined : readFile(certPem, "utf8"),
      keyPem === undefined ? undefined : readFile(keyPem, "utf8"),
    ]);
    const config = TlsConfig.fromPem({ ca, ...(cert === undefined ? {} : { cert }), ...(key === undefined ? {} : { key }) });
    if (certPem !== undefined && keyPem !== undefined && stamp !== undefined) {
      config.files = { cert: certPem, key: keyPem, stamp };
    }
    return config;
  }
}
