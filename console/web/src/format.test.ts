import { describe, expect, it } from 'vitest';
import { bytes, count, duration, percent, rate } from './format';

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
});
