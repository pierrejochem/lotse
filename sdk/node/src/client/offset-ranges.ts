import type { Range } from "../proto/messages.js";
import { asI64, asU64 } from "../record/unsigned.js";

/**
 * Offsets as the half-open ranges `[from, to)` that `Ack` and `Nack` carry: sorted as `u64`, duplicates dropped,
 * neighbours joined. `5, 6, 7, 9` is `[5, 8), [9, 10)`. Offsets are `bigint` bit patterns of `u64`.
 */
export function offsetRanges(offsets: readonly bigint[]): Range[] {
  const sorted = offsets.map(asU64).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const out: Range[] = [];
  let from = 0n;
  let to = 0n;
  let open = false;
  for (const o of sorted) {
    if (open && o === to - 1n) {
      continue; // a duplicate
    }
    if (open && o === to) {
      to = o + 1n;
      continue;
    }
    if (open) {
      out.push({ from: asI64(from), to: asI64(to) });
    }
    from = o;
    to = o + 1n;
    open = true;
  }
  if (open) {
    out.push({ from: asI64(from), to: asI64(to) });
  }
  return out;
}
