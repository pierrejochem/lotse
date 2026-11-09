import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Exchange, topicMatches } from "../src/client/exchange.js";
import type { BindingMetadata } from "../src/index.js";
import type { Header } from "../src/record/types.js";
import { field, hex, lines } from "./vectors.js";

/** Exchange routing against `routing.txt`, which `lotse-route` writes: the client routes with the same rules. */

/** `-` is null, `=hex` is bytes. */
function nullable(s: string): Uint8Array | null {
  return s === "-" ? null : hex(s.slice(1) === "" ? "-" : s.slice(1));
}

function text(s: string): string {
  return Buffer.from(nullable(s) as Uint8Array).toString("utf8");
}

function headers(s: string): Header[] {
  if (s === "-") {
    return [];
  }
  return s.split("+").map((h) => {
    const at = h.indexOf(":");
    return { key: hex(h.slice(0, at)), value: nullable(h.slice(at + 1)) };
  });
}

describe("exchange routing", () => {
  test("routes as the broker does, on every vector", () => {
    let n = 0;
    for (const v of lines("routing.txt")) {
      const bindings: BindingMetadata[] = [];
      if (field(v, 2) !== "-") {
        for (const b of field(v, 2).split(";")) {
          const f = b.split(",");
          const rest = f.slice(3).join(",");
          bindings.push({
            queueId: BigInt(f[0] as string),
            bindingKey: text(f[1] as string),
            headerMatch: headers(rest).map((h) => ({ key: h.key, value: h.value })),
            matchAll: f[2] === "all",
          });
        }
      }
      const x = new Exchange("x", 9n, Number(field(v, 1)), bindings);
      const want = field(v, 5) === "-" ? [] : field(v, 5).split(",").map(BigInt);
      assert.deepEqual(x.route(text(field(v, 3)), headers(field(v, 4))), want, field(v, 0));
      n++;
    }
    assert.equal(n, 41, "every vector read");
  });

  test("an exchange of a type this library does not know routes nowhere", () => {
    const x = new Exchange("x", 9n, 7, [{ queueId: 1n, bindingKey: "k", headerMatch: [], matchAll: true }]);
    assert.deepEqual(x.route("k", []), []);
  });

  test("a direct exchange compares whole keys, in either direction", () => {
    const direct = (key: string): Exchange =>
      new Exchange("x", 9n, 0, [{ queueId: 1n, bindingKey: key, headerMatch: [], matchAll: true }]);
    assert.deepEqual(direct("a").route("a", []), [1n]);
    assert.deepEqual(direct("a").route("a.b", []), [], "the binding key is a prefix of the routing key");
    assert.deepEqual(direct("a.b").route("a", []), [], "the routing key is a prefix of the binding key");
    assert.deepEqual(direct("a").route("ab", []), []);
    assert.deepEqual(direct("").route("", []), [1n]);
    assert.deepEqual(direct("").route("a", []), []);
  });

  test("a queue bound twice gets a message once", () => {
    const x = new Exchange("x", 9n, 1, [
      { queueId: 1n, bindingKey: "a.*", headerMatch: [], matchAll: true },
      { queueId: 1n, bindingKey: "#", headerMatch: [], matchAll: true },
    ]);
    assert.deepEqual(x.route("a.b", []), [1n]);
  });

  test("queue ids are ordered as u64", () => {
    const big = -2n; // 0xFFFF_FFFF_FFFF_FFFE
    const x = new Exchange("x", 9n, 2, [
      { queueId: big, bindingKey: "", headerMatch: [], matchAll: true },
      { queueId: 3n, bindingKey: "", headerMatch: [], matchAll: true },
    ]);
    assert.deepEqual(x.route("", []), [3n, big]);
  });

  test("topic words: # takes zero or more, * exactly one, an empty key has no words", () => {
    assert.ok(topicMatches("a.#", "a"));
    assert.ok(topicMatches("#", ""));
    assert.ok(!topicMatches("*", ""));
    assert.ok(topicMatches("a.*.c", "a.b.c"));
    assert.ok(!topicMatches("a.*.c", "a.b.b.c"));
  });
});
