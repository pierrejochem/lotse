import { describe, expect, it } from 'vitest';
import { deriveCredential, SCRAM_ITERATIONS } from './scramCredential';

// The same literals as `the_credential_matches_the_browsers_vector` in
// crates/lotse-auth/src/tests.rs, computed outside both code bases.
describe('deriveCredential', () => {
  it('matches the broker side for the shared vector', async () => {
    const salt = Uint8Array.from({ length: 16 }, (_, i) => i);
    const c = await deriveCredential('correct horse battery staple', salt);
    expect(c).toEqual({
      salt: 'AAECAwQFBgcICQoLDA0ODw==',
      iterations: SCRAM_ITERATIONS,
      stored_key: '133LyyyTZ9u5IxaWPaWk5l5TKP2fEQFyDZSCuv3A0BNEgOl12WJueHXEA4U0zvd8TvaaXp8dVHFJTV2VvQOaAg==',
      server_key: 'g8+8PdM5XuazUPbNGNF2tHolTXi0sKq726W8O6Rxo6ojgek5eSxEeRvKezIBKIr9zgQiZ0TMBHNhodkJr8OyFw==',
    });
  });

  it('uses a fresh salt each time', async () => {
    const [a, b] = await Promise.all([deriveCredential('pw'), deriveCredential('pw')]);
    expect(a.salt).not.toEqual(b.salt);
    expect(atob(a.salt)).toHaveLength(16);
  });
});
