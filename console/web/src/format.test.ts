import { describe, expect, it } from 'vitest';
import { NONE, bytes, count, counted, duration, fraction, ms, percent, rate } from './format';

describe('format', () => {
  it('groups thousands', () => {
    expect(count(1284)).toBe('1,284');
    expect(count(18_442_107)).toBe('18,442,107');
  });

  it('uses binary units with three significant digits', () => {
    expect(bytes(512)).toBe('512 B');
    expect(bytes(418 * 2 ** 30)).toBe('418 GiB');
    expect(bytes(1.62 * 2 ** 40)).toBe('1.62 TiB');
    expect(rate(48.2 * 2 ** 20)).toBe('48.2 MiB/s');
  });

  it('formats durations with the two largest units', () => {
    expect(duration((6 * 86_400 + 22 * 3600) * 1000)).toBe('6 d 22 h');
    expect(duration(130_000)).toBe('2 min 10 s');
    expect(duration(400)).toBe('0.4 s');
  });

  it('formats fractions as percent', () => {
    expect(percent(0.19)).toBe('19 %');
  });

  it('shows what is not reported as a dash', () => {
    for (const v of [null, undefined, Number.NaN]) {
      expect([count(v), bytes(v), rate(v), percent(v), duration(v), ms(v)]).toEqual(Array(6).fill(NONE));
    }
    expect(fraction(19, 100)).toBe(0.19);
    expect(fraction(null, 100)).toBeNull();
    expect(fraction(1, 0)).toBeNull();
    expect(ms(7.84)).toBe('7.8 ms');
    expect([counted(1, 'stream'), counted(3, 'stream'), counted(2, 'consumer', 'consumers')]).toEqual(['1 stream', '3 streams', '2 consumers']);
  });
});
