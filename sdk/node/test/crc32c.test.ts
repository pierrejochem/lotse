import assert from "node:assert/strict";
import { test } from "node:test";
import { crc32c } from "../src/record/crc32c.js";
import { field, hex, lines } from "./vectors.js";

test("CRC32C: the broker's vectors", () => {
  for (const v of lines("crc32c.txt")) {
    const input = hex(field(v, 1));
    assert.equal(crc32c(input), parseInt(field(v, 2), 16), field(v, 0));
  }
});

test("CRC32C: the standard check value of 123456789", () => {
  assert.equal(crc32c(new TextEncoder().encode("123456789")), 0xe3069283);
});

test("CRC32C: a range is checksummed without the bytes around it", () => {
  const b = new TextEncoder().encode("xx123456789yy");
  assert.equal(crc32c(b, 2, 11), 0xe3069283);
});
