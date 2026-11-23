import { describe, expect, it } from 'vitest';
import { hexDump } from './Messages';

describe('hexDump', () => {
  it('writes the UTF-8 bytes sixteen to a line with the offset in hex', () => {
    const lines = hexDump('{"id": "order-1", "total": 12.5}').split('\n');
    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe('00000000  7b 22 69 64 22 3a 20 22 6f 72 64 65 72 2d 31 22');
    expect(lines[1]?.startsWith('00000010  ')).toBe(true);
  });

  it('counts bytes, not characters', () => {
    expect(hexDump('é')).toBe('00000000  c3 a9');
    expect(hexDump('')).toBe('');
  });
});
