// Formatting for numbers the API sends raw.

const nf = new Intl.NumberFormat('en-US');

/** 1284 → "1,284". */
export function count(n: number): string {
  return nf.format(Math.round(n));
}

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'] as const;

/** Binary units: 449_000_000_000 → "418 GiB". Three significant digits. */
export function bytes(n: number): string {
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
export function rate(bytesPerSec: number): string {
  return `${bytes(bytesPerSec)}/s`;
}

/** Fraction to percent: 0.19 → "19 %". */
export function percent(f: number, digits = 0): string {
  return `${(f * 100).toFixed(digits)} %`;
}

/** Milliseconds to the two largest units: 600_000_000 → "6 d 22 h". */
export function duration(ms: number): string {
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
