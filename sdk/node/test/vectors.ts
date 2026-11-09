import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fromHex, toHex } from "../src/record/bytes.js";
import { crc32c } from "../src/record/crc32c.js";
import type { BatchRecord } from "../src/record/types.js";
import { u64ToString } from "../src/record/unsigned.js";

/** Reads the golden vectors that the Rust side writes to `sdk/conformance`. */

const DIR = process.env["LOTSE_CONFORMANCE"] ?? fileURLToPath(new URL("../../../conformance/", import.meta.url));

/** The lines of a vector file split at spaces, comments and blank lines left out. */
export function lines(file: string): string[][] {
  const out: string[][] = [];
  for (const line of readFileSync(`${DIR}/${file}`, "utf8").split("\n")) {
    if (line.trim() !== "" && !line.startsWith("#")) {
      out.push(line.split(" "));
    }
  }
  if (out.length === 0) {
    throw new Error(`${file} holds no vectors`);
  }
  return out;
}

/** `-` is the empty byte string. */
export function hex(s: string): Uint8Array {
  return s === "-" ? new Uint8Array(0) : fromHex(s);
}

export { toHex };

/** Field `i` of a vector line; fails the test run, not with `undefined`, when the line is too short. */
export function field(v: string[], i: number): string {
  const s = v[i];
  if (s === undefined) {
    throw new Error(`vector ${v[0]} has no field ${i}`);
  }
  return s;
}

function nullable(b: Uint8Array | null): string {
  if (b === null) {
    return "-";
  }
  if (b.length > 64) {
    return `#${b.length}:${crc32c(b).toString(16).padStart(8, "0")}`;
  }
  return `=${toHex(b)}`;
}

/** A record in the notation of `batch-records.txt`. */
export function recordLine(label: string, index: number, r: BatchRecord): string {
  const headers =
    r.headers.length === 0 ? "-" : r.headers.map((h) => `${toHex(h.key)}:${nullable(h.value)}`).join(",");
  return [label, index, u64ToString(r.offset), r.timestamp, nullable(r.key), nullable(r.value), headers].join(" ");
}

