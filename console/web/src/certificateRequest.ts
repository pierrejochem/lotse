/**
 * A client certificate request made in the browser (ADR 0092): an ECDSA
 * P-256 key pair from WebCrypto and a PKCS#10 signing request signed with
 * it. The private key is handed to the person as a PKCS#8 file and goes no
 * further; the console and the cluster hold only the request. WebCrypto
 * cannot write a PKCS#10 request, so the few DER pieces it needs are here.
 */

export interface CertificateRequest {
  /** The signing request, DER, base64: what the console passes on. */
  csr: string;
  /** The private key as a PKCS#8 PEM file; keep it, nothing else has it. */
  privateKeyPem: string;
  /** SHA-256 of the public key, hex: `lotse certs --sign` shows the same. */
  fingerprint: string;
}

/** WebCrypto exists only in secure contexts: https, or localhost. */
export const canRequest = (): boolean => typeof crypto !== 'undefined' && crypto.subtle != null;

/** The characters a principal may have, as the broker checks them. */
export const principalError = (name: string): string | null => {
  if (name.length === 0) return 'Enter the principal the certificate is for.';
  if (name.length > 128) return 'A principal has at most 128 characters.';
  if (name.startsWith('.')) return 'A principal does not start with a dot.';
  if (!/^[A-Za-z0-9._@-]+$/.test(name)) return 'Use letters, digits, “.”, “_”, “-” and “@” only.';
  return null;
};

const concat = (...parts: Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
};

/** One DER element: tag, length, content. */
export function der(tag: number, content: Uint8Array): Uint8Array {
  const n = content.length;
  const length =
    n < 0x80 ? [n] : n < 0x100 ? [0x81, n] : n < 0x10000 ? [0x82, n >> 8, n & 0xff] : null;
  if (length === null) throw new Error('too long for a signing request');
  return concat(new Uint8Array([tag, ...length]), content);
}

const oid = (bytes: number[]) => der(0x06, new Uint8Array(bytes));
// 2.5.4.3 commonName; 1.2.840.10045.4.3.2 ecdsa-with-SHA256.
const OID_COMMON_NAME = oid([0x55, 0x04, 0x03]);
const OID_ECDSA_SHA256 = oid([0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]);

/** An unsigned INTEGER from big-endian bytes: no leading zeros, a 0 in front if the top bit is set. */
const integer = (be: Uint8Array): Uint8Array => {
  let i = 0;
  while (i < be.length - 1 && be[i] === 0) i++;
  const v = be.slice(i);
  return der(0x02, (v[0] ?? 0) & 0x80 ? concat(new Uint8Array([0]), v) : v);
};

/** WebCrypto's ECDSA signature is r followed by s; X.509 wants SEQUENCE { r, s }. */
export const ecdsaToDer = (raw: Uint8Array): Uint8Array => {
  const half = raw.length / 2;
  return der(0x30, concat(integer(raw.slice(0, half)), integer(raw.slice(half))));
};

const toBase64 = (bytes: Uint8Array): string => {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
};

const hex = (bytes: Uint8Array): string => Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');

/** Bytes as a PEM block with 64 characters to a line. */
export const pem = (label: string, bytes: Uint8Array): string =>
  `-----BEGIN ${label}-----\n${toBase64(bytes).match(/.{1,64}/g)!.join('\n')}\n-----END ${label}-----\n`;

/** The signing request for `principal` and the key it names, signed with it. */
export async function signingRequest(principal: string, keys: CryptoKeyPair): Promise<Uint8Array> {
  const spki = new Uint8Array(await crypto.subtle.exportKey('spki', keys.publicKey));
  const name = der(0x30, der(0x31, der(0x30, concat(OID_COMMON_NAME, der(0x0c, new TextEncoder().encode(principal))))));
  // CertificationRequestInfo: version 0, subject, key, no attributes.
  const info = der(0x30, concat(der(0x02, new Uint8Array([0])), name, spki, der(0xa0, new Uint8Array())));
  const signature = new Uint8Array(
    await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, keys.privateKey, info),
  );
  const algorithm = der(0x30, OID_ECDSA_SHA256);
  return der(0x30, concat(info, algorithm, der(0x03, concat(new Uint8Array([0]), ecdsaToDer(signature)))));
}

/** Makes a key pair and the request for `principal`. */
export async function makeCertificateRequest(principal: string): Promise<CertificateRequest> {
  if (!canRequest()) throw new Error('This page cannot make a key here: open the console over https or on localhost.');
  const error = principalError(principal);
  if (error) throw new Error(error);
  const keys = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
  const csr = await signingRequest(principal, keys);
  const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', keys.privateKey));
  const point = new Uint8Array(await crypto.subtle.exportKey('raw', keys.publicKey));
  const fingerprint = hex(new Uint8Array(await crypto.subtle.digest('SHA-256', point)));
  return { csr: toBase64(csr), privateKeyPem: pem('PRIVATE KEY', pkcs8), fingerprint };
}
