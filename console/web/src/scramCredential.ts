/**
 * SCRAM-SHA-512 credentials, derived in the browser (ADR 0089): the password
 * never leaves the user's machine, the console passes on only what the broker
 * stores. The same steps as `lotse_auth::scram::ScramCredential::new`; the
 * password is used as its UTF-8 bytes, as there.
 */

/** The iterations the broker requires at least; the client uses the same. */
export const SCRAM_ITERATIONS = 4096;

export interface Credential {
  salt: string;
  iterations: number;
  stored_key: string;
  server_key: string;
}

const text = new TextEncoder();

const toBase64 = (bytes: Uint8Array): string => {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
};

/** WebCrypto exists only in secure contexts: https, or localhost. */
export const canDerive = (): boolean => typeof crypto !== 'undefined' && crypto.subtle != null;

async function hmac(key: Uint8Array, data: string): Promise<Uint8Array> {
  const k = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-512' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', k, text.encode(data)));
}

/** The credential for `password` with a given salt; the salt is random unless a test passes one. */
export async function deriveCredential(
  password: string,
  salt: Uint8Array = crypto.getRandomValues(new Uint8Array(16)),
  iterations: number = SCRAM_ITERATIONS,
): Promise<Credential> {
  if (!canDerive()) throw new Error('This page cannot derive a password here: open the console over https or on localhost.');
  const base = await crypto.subtle.importKey('raw', text.encode(password), 'PBKDF2', false, ['deriveBits']);
  const salted = new Uint8Array(
    await crypto.subtle.deriveBits({ name: 'PBKDF2', hash: 'SHA-512', salt, iterations }, base, 512),
  );
  const clientKey = await hmac(salted, 'Client Key');
  const stored = new Uint8Array(await crypto.subtle.digest('SHA-512', clientKey));
  const server = await hmac(salted, 'Server Key');
  return { salt: toBase64(salt), iterations, stored_key: toBase64(stored), server_key: toBase64(server) };
}
