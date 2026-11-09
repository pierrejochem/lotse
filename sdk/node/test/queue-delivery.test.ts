import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Delivery, type Header, offsetRanges } from "../src/index.js";

/** Offset ranges and the typed view of a delivery. */

const h = (k: string, v: string | null): Header => ({ key: Buffer.from(k), value: v === null ? null : Buffer.from(v) });

const delivery = (...headers: Header[]): Delivery =>
  new Delivery({ offset: 5n, timestamp: 1_000n, key: null, value: Buffer.from("body"), headers }, 3, {});

describe("offset ranges", () => {
  test("neighbours join and gaps stay", () => {
    assert.deepEqual(offsetRanges([5n, 6n, 7n, 9n]), [
      { from: 5n, to: 8n },
      { from: 9n, to: 10n },
    ]);
  });

  test("they are sorted, and duplicates dropped", () => {
    assert.deepEqual(offsetRanges([9n, 5n, 7n, 6n, 7n, 5n]), [
      { from: 5n, to: 8n },
      { from: 9n, to: 10n },
    ]);
    assert.deepEqual(offsetRanges([]), []);
    assert.deepEqual(offsetRanges([3n]), [{ from: 3n, to: 4n }]);
  });

  test("offsets order as u64: a bit pattern with the top bit set is the largest", () => {
    const top = -2n; // 2^64 - 2
    assert.deepEqual(offsetRanges([top, 1n, -1n]), [
      { from: 1n, to: 2n },
      { from: -2n, to: 0n }, // [2^64 - 2, 2^64) as bit patterns: the end wraps to 0
    ]);
  });
});

describe("a delivery", () => {
  test("a message without dead-letter headers is not dead-lettered", () => {
    assert.equal(delivery(h("x-other", "1")).deadLetter(), null);
    assert.equal(delivery().deadLetter(), null);
  });

  test("the dead-letter headers are read as typed", () => {
    const d = delivery(
      h("app", "yes"),
      h("x-lotse-dead-letter-reason", "max-deliveries"),
      h("x-lotse-dead-letter-queue", "jobs"),
      h("x-lotse-dead-letter-queue-id", "18446744073709551615"),
      h("x-lotse-dead-letter-offset", "42"),
      h("x-lotse-dead-letter-deliveries", "3"),
      h("x-lotse-dead-letter-floor", "40"),
    );
    assert.deepEqual(d.deadLetter(), {
      reason: "max-deliveries",
      queue: "jobs",
      queueId: -1n, // a u64 above i64::MAX, as its bit pattern
      offset: 42n,
      deliveries: 3,
      floor: 40n,
    });
    assert.equal(d.offset, 5n);
    assert.equal(d.deliveryCount, 3);
  });

  test("an unreadable number reads as zero and a missing name as empty", () => {
    const dl = delivery(
      h("x-lotse-dead-letter-reason", "expired"),
      h("x-lotse-dead-letter-offset", "nope"),
      h("x-lotse-dead-letter-floor", "18446744073709551616"),
      h("x-lotse-dead-letter-queue-id", "-3"),
    ).deadLetter();
    assert.equal(dl?.reason, "expired");
    assert.equal(dl?.offset, 0n);
    assert.equal(dl?.floor, 0n, "more than a u64");
    assert.equal(dl?.queueId, 0n, "a sign is no digit");
    assert.equal(dl?.queue, "");
  });

  test("a header without a value is skipped and the last of a name wins", () => {
    const dl = delivery(
      h("x-lotse-dead-letter-reason", "max-deliveries"),
      h("x-lotse-dead-letter-reason", null),
      h("x-lotse-dead-letter-queue", "old"),
      h("x-lotse-dead-letter-queue", "new"),
    ).deadLetter();
    assert.equal(dl?.reason, "max-deliveries");
    assert.equal(dl?.queue, "new");
  });

  test("headers keep their order and the text is the value", () => {
    const d = delivery(h("a", "1"), h("b", "2"));
    assert.deepEqual(
      d.headers.map((x) => Buffer.from(x.key).toString()),
      ["a", "b"],
    );
    assert.equal(d.text(), "body");
    assert.equal(d.timestamp, 1_000n);
    assert.equal(d.key, null);
  });
});
