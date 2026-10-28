// Formatting for numbers the API sends raw. A value the cluster does not
// report (yet) arrives as null and shows as NONE.

const nf = new Intl.NumberFormat('en-US');

/** A number that may not be reported. */
export type Maybe = number | null | undefined;

/** Shown for a value the cluster does not report (yet). */
export const NONE = '—';

/** What NONE means, for a note next to it. */
export const NONE_NOTE = '— means the cluster does not report this (yet). Rates need two samples, a few seconds apart.';

function known(n: Maybe): n is number {
  return n !== null && n !== undefined && Number.isFinite(n);
}

/** 1284 → "1,284". */
export function count(n: Maybe): string {
  return known(n) ? nf.format(Math.round(n)) : NONE;
}

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'] as const;

/** "1 stream", "3 streams": a count and its noun. */
export function counted(n: Maybe, one: string, many = `${one}s`): string {
  return known(n) ? `${count(n)} ${n === 1 ? one : many}` : NONE;
}

/** Binary units: 449_000_000_000 → "418 GiB". Three significant digits. */
export function bytes(n: Maybe): string {
  if (!known(n)) return NONE;
  let v = n;
  let i = 0;
  while (v >= 1024 && i < UNITS.length - 1) {
    v /= 1024;
    i++;
  }
  const digits = i === 0 ? 0 : v >= 100 ? 0 : v >= 10 ? 1 : 2;
  return `${v.toFixed(digits)} ${UNITS[i]}`;
}

/** Bytes per second: "48.2 MiB/s". */
export function rate(bytesPerSec: Maybe): string {
  return known(bytesPerSec) ? `${bytes(bytesPerSec)}/s` : NONE;
}

/** Fraction to percent: 0.19 → "19 %". */
export function percent(f: Maybe, digits = 0): string {
  return known(f) ? `${(f * 100).toFixed(digits)} %` : NONE;
}

/** Part of a whole, if both are known: free / total → 0.19. */
export function fraction(part: Maybe, whole: Maybe): number | null {
  return known(part) && known(whole) && whole > 0 ? part / whole : null;
}

/** Milliseconds with one decimal: "7.8 ms". */
export function ms(n: Maybe): string {
  return known(n) ? `${n.toFixed(1)} ms` : NONE;
}

/** Milliseconds to the two largest units: 600_000_000 → "6 d 22 h". */
export function duration(ms: Maybe): string {
  if (!known(ms)) return NONE;
  if (ms < 1000) return `${(ms / 1000).toFixed(1)} s`;
  const parts: [number, string][] = [
    [86_400_000, 'd'],
    [3_600_000, 'h'],
    [60_000, 'min'],
    [1_000, 's'],
  ];
  const out: string[] = [];
  let rest = ms;
  for (const [size, unit] of parts) {
    if (rest >= size) {
      out.push(`${Math.floor(rest / size)} ${unit}`);
      rest %= size;
    }
    if (out.length === 2) break;
  }
  return out.join(' ');
}

/** RFC 3339 → local "HH:MM:SS". */
export function clock(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-GB', { hour12: false });
}

/** RFC 3339 → local "HH:MM:SS.mmm". */
export function clockMs(iso: string): string {
  const d = new Date(iso);
  return `${clock(iso)}.${String(d.getMilliseconds()).padStart(3, '0')}`;
}
