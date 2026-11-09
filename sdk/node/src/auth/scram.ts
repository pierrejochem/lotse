import { createHash, createHmac, pbkdf2, pbkdf2Sync, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";

/** The primitives of SCRAM-SHA-512 on raw bytes. */

const pbkdf2Async = promisify(pbkdf2);

export const CLIENT_KEY = Buffer.from("Client Key", "ascii");
export const SERVER_KEY = Buffer.from("Server Key", "ascii");

export function hmac(key: Uint8Array, data: Uint8Array): Uint8Array {
  // HMAC zero-pads a short key to the block size, so an empty key is the same key as 128 zero bytes.
  return createHmac("sha512", key).update(data).digest();
}

export function sha512(data: Uint8Array): Uint8Array {
  return createHash("sha512").update(data).digest();
}

/** PBKDF2-HMAC-SHA512 with a 64-byte output. Blocks for the iterations: up to a second or two at the maximum. */
export function saltedPasswordSync(password: Uint8Array, salt: Uint8Array, iterations: number): Uint8Array {
  return pbkdf2Sync(password, salt, iterations, 64, "sha512");
}

/** The same, off the event loop. */
export async function saltedPassword(password: Uint8Array, salt: Uint8Array, iterations: number): Promise<Uint8Array> {
  return pbkdf2Async(password, salt, iterations, 64, "sha512");
}

export function xor(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length);
  for (let i = 0; i < a.length; i++) {
    out[i] = (a[i] as number) ^ (b[i] as number);
  }
  return out;
}

export function constantTimeEquals(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && timingSafeEqual(a, b);
}
