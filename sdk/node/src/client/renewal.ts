import { X509Certificate, generateKeyPairSync, sign } from "node:crypto";
import { chmodSync, readFileSync, renameSync, writeFileSync } from "node:fs";

/**
 * Renewing a client certificate over a connection made with it (ADR 0123): when a certificate may be renewed, a new
 * key with its signing request, and writing the files a `TlsConfig` reads.
 *
 * A certificate may be renewed in its last 30 days or once two thirds of its validity have passed, whichever comes
 * first, until it expires. The new certificate is valid for 90 days.
 */

/** A renewed client certificate. */
export interface RenewedCertificate {
  /** The new private key, PKCS#8 PEM. */
  readonly keyPem: string;
  /** The new certificate, then the renewal CA's, PEM. */
  readonly chainPem: string;
  /** Milliseconds since the Unix epoch at which the new certificate stops being valid. */
  readonly notAfterMs: bigint;
}

/** The last stretch of a certificate's validity in which it may always be renewed: 30 days. */
export const RENEWAL_WINDOW_MS = 30n * 86_400_000n;

/**
 * The millisecond since the Unix epoch from which a certificate valid from `notBeforeMs` until `notAfterMs` may be
 * renewed: the earlier of its last 30 days and two thirds of its validity.
 */
export function renewalWindowOpensMs(notBeforeMs: bigint, notAfterMs: bigint): bigint {
  const span = notAfterMs > notBeforeMs ? notAfterMs - notBeforeMs : 0n;
  const twoThirds = notBeforeMs + (span / 3n) * 2n + ((span % 3n) * 2n) / 3n;
  const lastDays = notAfterMs > RENEWAL_WINDOW_MS ? notAfterMs - RENEWAL_WINDOW_MS : 0n;
  return lastDays < twoThirds ? lastDays : twoThirds;
}

/** Whether such a certificate may be renewed at `nowMs`: its window is open and it has not expired. */
export function mayRenew(notBeforeMs: bigint, notAfterMs: bigint, nowMs: bigint): boolean {
  return nowMs >= renewalWindowOpensMs(notBeforeMs, notAfterMs) && nowMs < notAfterMs;
}

/** Whether the certificate in a PEM file (its first block, the leaf) may be renewed at `nowMs`. */
export function renewalDue(certPemFile: string, nowMs: bigint = BigInt(Date.now())): boolean {
  const leaf = new X509Certificate(readFileSync(certPemFile));
  return mayRenew(BigInt(Date.parse(leaf.validFrom)), BigInt(Date.parse(leaf.validTo)), nowMs);
}

/** A new ECDSA P-256 key, PKCS#8 PEM, and a PKCS#10 signing request for it, DER; the node ignores its subject. */
export function newRenewalKey(): { readonly keyPem: string; readonly csr: Uint8Array } {
  const { publicKey, privateKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
  const cn = new TextEncoder().encode("renewal");
  const name = tlv(0x30, tlv(0x31, tlv(0x30, COMMON_NAME, tlv(0x0c, cn))));
  const spki = new Uint8Array(publicKey.export({ type: "spki", format: "der" }));
  const info = tlv(0x30, Uint8Array.of(0x02, 0x01, 0x00), name, spki, Uint8Array.of(0xa0, 0x00));
  const sig = new Uint8Array(sign("sha256", info, { key: privateKey, dsaEncoding: "der" }));
  const csr = tlv(0x30, info, tlv(0x30, ECDSA_WITH_SHA256), tlv(0x03, Uint8Array.of(0), sig));
  return { keyPem: privateKey.export({ type: "pkcs8", format: "pem" }).toString(), csr };
}

/** PEM text of one DER block. */
export function pem(label: string, der: Uint8Array): string {
  const b64 = Buffer.from(der).toString("base64");
  const lines = b64.match(/.{1,64}/g) ?? [];
  return `-----BEGIN ${label}-----\n${lines.join("\n")}\n-----END ${label}-----\n`;
}

/**
 * Writes the renewed key and chain over the files: the key first, each through a file beside it that is then moved
 * over the old one, so a reader sees one file or the other, never half of one. A `TlsConfig` read with `fromFiles`
 * from them takes up the pair once both are written.
 */
export function writeRenewed(renewed: RenewedCertificate, certPemFile: string, keyPemFile: string): void {
  replace(keyPemFile, renewed.keyPem, true);
  replace(certPemFile, renewed.chainPem, false);
}

function replace(path: string, text: string, secret: boolean): void {
  const tmp = `${path}.new`;
  writeFileSync(tmp, text, { encoding: "ascii", mode: secret ? 0o600 : 0o644 });
  if (secret) {
    chmodSync(tmp, 0o600);
  }
  renameSync(tmp, path);
}

/** OID 2.5.4.3, commonName. */
const COMMON_NAME = Uint8Array.of(0x06, 0x03, 0x55, 0x04, 0x03);
/** OID 1.2.840.10045.4.3.2, ecdsa-with-SHA256. */
const ECDSA_WITH_SHA256 = Uint8Array.of(0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02);

/** A DER value: tag, definite length, then the parts. */
function tlv(tag: number, ...parts: Uint8Array[]): Uint8Array {
  const n = parts.reduce((sum, p) => sum + p.length, 0);
  const head = n < 0x80 ? [tag, n] : n < 0x100 ? [tag, 0x81, n] : [tag, 0x82, n >> 8, n & 0xff];
  const out = new Uint8Array(head.length + n);
  out.set(head, 0);
  let at = head.length;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}
