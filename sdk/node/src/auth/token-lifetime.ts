/**
 * How long an access token lives (ADR 0133): one of five steps, counted from when the metadata leader records it. The
 * value is the code that travels in `CreateToken`.
 */
export const TokenLifetime = {
  /** 7 days. */
  WEEK: 1,
  /** 30 days. */
  MONTH: 2,
  /** 90 days. */
  QUARTER: 3,
  /** 180 days. */
  HALF_YEAR: 4,
  /** 365 days. */
  YEAR: 5,
} as const;

export type TokenLifetime = (typeof TokenLifetime)[keyof typeof TokenLifetime];

const DAYS = [0, 7, 30, 90, 180, 365] as const;

/**
 * The lifetime in days.
 *
 * @throws RangeError for anything but the codes 1 to 5
 */
export function tokenLifetimeDays(lifetime: number): number {
  const days = Number.isInteger(lifetime) ? DAYS[lifetime] : undefined;
  if (days === undefined || lifetime === 0) {
    throw new RangeError(`a token lifetime's code is 1 to 5, got ${lifetime}`);
  }
  return days;
}
