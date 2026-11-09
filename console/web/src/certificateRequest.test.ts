import { describe, expect, it } from 'vitest';
import { ecdsaToDer, makeCertificateRequest, principalError, signingRequest } from './certificateRequest';

/** Reads one DER element at `at`: its tag, content and the offset after it. */
function read(b: Uint8Array, at: number) {
  const byte = (i: number) => b[i] ?? 0;
  const tag = byte(at);
  let n = byte(at + 1);
  let start = at + 2;
  if (n === 0x81) [n, start] = [byte(at + 2), at + 3];
  else if (n === 0x82) [n, start] = [(byte(at + 2) << 8) | byte(at + 3), at + 4];
  return { tag, whole: b.slice(at, start + n), content: b.slice(start, start + n), next: start + n };
}

/** The elements inside a constructed one. */
type Elem = ReturnType<typeof read>;
function children(content: Uint8Array): [Elem, Elem, Elem, Elem] {
  const out: Elem[] = [];
  for (let at = 0; at < content.length; ) {
    const e = read(content, at);
    out.push(e);
    at = e.next;
  }
  // Callers know how many elements the structure has.
  return out as [Elem, Elem, Elem, Elem];
}

/** X.509's SEQUENCE { r, s } back to WebCrypto's r followed by s, 32 bytes each. */
function derToRaw(sig: Uint8Array): Uint8Array {
  const [r = new Uint8Array(), s = new Uint8Array()] = children(read(sig, 0).content).slice(0, 2).map((e) => {
    let v = e.content;
    while (v.length > 32) v = v.slice(1);
    const out = new Uint8Array(32);
    out.set(v, 32 - v.length);
    return out;
  });
  return Uint8Array.from([...r, ...s]);
}

const fromBase64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

describe('signingRequest', () => {
  it('is a PKCS#10 request for the key, named by the principal and signed with that key', async () => {
    const keys = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
    const csr = await signingRequest('alice@example', keys);
    const [info, algorithm, signature] = children(read(csr, 0).content);
    expect(read(csr, 0).next).toBe(csr.length);
    const [version, subject, spki, attributes] = children(info.content);
    expect([version.tag, attributes.tag]).toEqual([0x02, 0xa0]);
    // SEQUENCE { SET { SEQUENCE { commonName, UTF8String "alice@example" } } }
    const [, value] = children(children(read(subject.content, 0).content)[0].content);
    expect(new TextDecoder().decode(value.content)).toBe('alice@example');
    expect(spki.whole).toEqual(new Uint8Array(await crypto.subtle.exportKey('spki', keys.publicKey)));
    // ecdsa-with-SHA256, with no parameters.
    expect(Array.from(children(algorithm.content)[0].content)).toEqual([0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]);
    expect(children(algorithm.content)).toHaveLength(1);
    // The signature, after the BIT STRING's unused-bits byte, verifies over the info.
    expect(signature.tag).toBe(0x03);
    expect(signature.content[0]).toBe(0);
    const raw = derToRaw(signature.content.slice(1));
    expect(await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, keys.publicKey, raw, info.whole)).toBe(true);
  });

  it('writes integers the way DER wants them', () => {
    // r with the top bit set needs a 0 in front; leading zeros are dropped.
    const raw = new Uint8Array(64);
    raw[0] = 0x80;
    raw[31] = 1;
    raw[32] = 0;
    raw[33] = 0x7f;
    raw[63] = 2;
    const [r, s] = children(read(ecdsaToDer(raw), 0).content);
    expect(r.content).toHaveLength(33);
    expect(r.content[0]).toBe(0);
    expect(s.content[0]).toBe(0x7f);
    expect(s.content).toHaveLength(31);
  });
});

describe('makeCertificateRequest', () => {
  it('hands out the private key as PKCS#8 and a fingerprint of the public key', async () => {
    const made = await makeCertificateRequest('ingest-service');
    expect(made.privateKeyPem).toMatch(/^-----BEGIN PRIVATE KEY-----\n[A-Za-z0-9+/=\n]+-----END PRIVATE KEY-----\n$/);
    expect(made.fingerprint).toMatch(/^[0-9a-f]{64}$/);
    const csr = fromBase64(made.csr);
    expect(csr[0]).toBe(0x30);
    // The key in the request is the one the fingerprint names.
    const spki = children(children(read(csr, 0).content)[0].content)[2];
    const point = spki.content.slice(-65);
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', point));
    expect(Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join('')).toBe(made.fingerprint);
  });

  it('refuses names the broker would refuse', async () => {
    for (const bad of ['', '.hidden', 'a b', '../x', 'x'.repeat(129)]) {
      expect(principalError(bad)).not.toBeNull();
      await expect(makeCertificateRequest(bad)).rejects.toThrow();
    }
    expect(principalError('alice@example.org')).toBeNull();
  });
});
