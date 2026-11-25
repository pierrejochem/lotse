import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ApiKey,
  DecodeError,
  decodePush,
  decodeRequest,
  decodeResponse,
  encodePush,
  encodeRequest,
  encodeResponse,
  ErrorCode,
  errorName,
  Flags,
  IMPLEMENTED_KEYS,
  isPush,
  isRetriable,
  type Request,
  responseCorrelationId,
  Writer,
} from "../src/proto/index.js";
import { field, hex, lines, toHex } from "./vectors.js";

/** Every frame the broker's codec wrote is decoded here and written again byte for byte. */

function decodeFails(f: () => unknown): DecodeError {
  try {
    f();
  } catch (e) {
    assert.ok(e instanceof DecodeError, `expected a DecodeError, got ${String(e)}`);
    return e;
  }
  assert.fail("expected a DecodeError");
}

test("every request decodes and is written again to the same bytes", () => {
  const vectors = lines("requests.hex");
  assert.ok(vectors.length >= 40, "the vectors cover every client request");
  for (const v of vectors) {
    if (tagged(v)) {
      continue; // aHeaderTagRidesOnItsFrame
    }
    const label = field(v, 0);
    const frame = hex(field(v, 1));
    const decoded = decodeRequest(frame);
    assert.deepEqual(decoded.readAtLeast, [], label);
    const correlationId = Number(label.split("-")[1]) + 1;
    assert.equal(decoded.header.correlationId, correlationId, label);
    assert.equal(decoded.header.apiVersion, 0, label);
    assert.equal(decoded.header.flags, 0, label);
    assert.equal(toHex(encodeRequest(correlationId, decoded.request)), field(v, 1), label);
  }
});

/** The samples of header tag 0 ride on a frame of their own, labelled `-tagged-` with correlation id 900. */
function tagged(v: string[]): boolean {
  return field(v, 0).includes("-tagged-");
}

/**
 * Header tag 0: `read_at_least` on a request, `applied` on a response, read as the broker wrote them and written
 * again byte for byte; an empty list writes no tag.
 */
test("aHeaderTagRidesOnItsFrame", () => {
  const req = lines("requests.hex").find(tagged);
  assert.ok(req !== undefined);
  const r = decodeRequest(hex(field(req, 1)));
  assert.equal(r.header.correlationId, 900);
  assert.deepEqual(r.request, { type: "DescribeNodes" });
  const wanted = [
    { group: 1n, index: 7n },
    { group: 2n, index: -1n },
  ];
  assert.deepEqual(r.readAtLeast, wanted, "u64::MAX is -1n, as every u64 here");
  assert.equal(toHex(encodeRequest(900, r.request, wanted)), field(req, 1));
  assert.deepEqual(encodeRequest(900, r.request, []), encodeRequest(900, r.request));

  const res = lines("responses.hex").find(tagged);
  assert.ok(res !== undefined);
  const d = decodeResponse(hex(field(res, 2)), Number(field(res, 1)));
  assert.equal(d.correlationId, 900);
  assert.deepEqual(d.response, { type: "Outcome", error: ErrorCode.NONE, leaderHint: null });
  assert.deepEqual(d.applied, [{ group: 1n, index: 9n }]);
  assert.equal(toHex(encodeResponse(900, d.response, d.applied)), field(res, 2));
});

/** A header index count larger than the field holds is refused, not trusted. */
test("aHeaderIndexCountBeyondTheFieldIsRefused", () => {
  const w = new Writer();
  w.u16(ApiKey.DESCRIBE_NODES).u16(0).u32(1).u16(0);
  w.tags([{ tag: 0, value: new Writer().u32(2).u64(1n).u64(7n).toBytes() }]);
  w.noTags();
  const e = decodeFails(() => decodeRequest(w.toBytes()));
  assert.equal(e.kind, "MALFORMED");
});

/** The zone reads' message tags: `Fetch` tag 1 `zone`, a fetched partition's tag 1 `preferredReplica` and `FindCoordinator`'s tag 0 `voters`. */
test("zoneTagsAreReadAsTheBrokerWroteThem", () => {
  let zone = false;
  for (const v of lines("requests.hex")) {
    const r: Request = decodeRequest(hex(field(v, 1))).request;
    if (r.type === "Fetch" && r.zone !== undefined) {
      zone = true;
      assert.equal(r.zone, "eu-1");
      assert.equal(r.allowFollower, true);
    }
  }
  let preferred = false;
  let voters = false;
  for (const v of lines("responses.hex")) {
    const r = decodeResponse(hex(field(v, 2)), Number(field(v, 1))).response;
    if (r.type === "Fetch") {
      for (const res of r.resources) {
        for (const p of res.partitions) {
          if (p.preferredReplica !== undefined) {
            preferred = true;
            assert.equal(p.preferredReplica, 2);
          }
        }
      }
    }
    if (r.type === "FindCoordinator" && r.voters !== undefined) {
      voters = true;
      assert.deepEqual(r.voters, [1, 2, 3]);
    }
  }
  assert.ok(zone && preferred && voters);
});

test("every client API key has a request vector", () => {
  const seen = new Set<number>();
  for (const v of lines("requests.hex")) {
    seen.add(decodeRequest(hex(field(v, 1))).header.apiKey);
  }
  for (const key of IMPLEMENTED_KEYS) {
    assert.ok(seen.has(key), `no vector for API key ${key}`);
  }
});

test("every response decodes and is written again to the same bytes", () => {
  const vectors = lines("responses.hex");
  assert.ok(vectors.length >= 40);
  for (const v of vectors) {
    if (tagged(v)) {
      continue; // aHeaderTagRidesOnItsFrame
    }
    const label = field(v, 0);
    const frame = hex(field(v, 2));
    const key = Number(field(v, 1));
    const decoded = decodeResponse(frame, key);
    assert.deepEqual(decoded.applied, [], label);
    const correlationId = Number(label.split("-")[1]);
    assert.equal(decoded.correlationId, correlationId, label);
    assert.equal(responseCorrelationId(frame), correlationId, label);
    assert.equal(toHex(encodeResponse(correlationId, decoded.response)), field(v, 2), label);
  }
});

test("every push decodes and is written again to the same bytes", () => {
  for (const v of lines("pushes.hex")) {
    const frame = hex(field(v, 1));
    assert.ok(isPush(frame), field(v, 0));
    assert.equal(toHex(encodePush(decodePush(frame))), field(v, 1), field(v, 0));
  }
});

test("the push layout puts the subscription id first in the body", () => {
  // correlation 0 | flags PUSH | no tags | api key 11 | version 0 | subscription id | error | ...
  const vectors = lines("pushes.hex");
  const frame = hex(field(vectors[0] as string[], 1));
  assert.deepEqual([...frame.subarray(0, 17)], [0, 0, 0, 0, 1, 0, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0]);
  const push = decodePush(frame);
  assert.equal(push.subscriptionId, 3);
  assert.equal(push.error, ErrorCode.NONE);
  assert.equal(push.deliveries.length, 2);
  assert.equal(push.deliveries[1]?.offset, 42n);
  assert.equal(push.deliveries[1]?.deliveryCount, 3);
  const ended = decodePush(hex(field(vectors[1] as string[], 1)));
  assert.equal(ended.error, ErrorCode.NOT_LEADER);
  assert.deepEqual(ended.leaderHint, { leader: 1, term: 7n });
});

test("a frame cut at every length is refused", () => {
  for (const v of lines("requests.hex")) {
    const frame = hex(field(v, 1));
    for (let cut = 0; cut < frame.length; cut++) {
      decodeFails(() => decodeRequest(frame.subarray(0, cut)));
    }
  }
  for (const v of lines("responses.hex")) {
    const frame = hex(field(v, 2));
    const key = Number(field(v, 1));
    for (let cut = 0; cut < frame.length; cut++) {
      decodeFails(() => decodeResponse(frame.subarray(0, cut), key));
    }
  }
  for (const v of lines("pushes.hex")) {
    const frame = hex(field(v, 1));
    for (let cut = 0; cut < frame.length; cut++) {
      decodeFails(() => decodePush(frame.subarray(0, cut)));
    }
  }
});

function plusOne(frame: Uint8Array): Uint8Array {
  const out = new Uint8Array(frame.length + 1);
  out.set(frame);
  return out;
}

test("a frame with a byte too many is refused as malformed", () => {
  for (const v of lines("requests.hex")) {
    const e = decodeFails(() => decodeRequest(plusOne(hex(field(v, 1)))));
    assert.equal(e.kind, "MALFORMED", field(v, 0));
  }
  for (const v of lines("responses.hex")) {
    const e = decodeFails(() => decodeResponse(plusOne(hex(field(v, 2))), Number(field(v, 1))));
    assert.equal(e.kind, "MALFORMED", field(v, 0));
  }
  for (const v of lines("pushes.hex")) {
    decodeFails(() => decodePush(plusOne(hex(field(v, 1)))));
  }
});

test("error codes have the broker's names and retry classes", () => {
  const vectors = lines("error-codes.txt");
  assert.equal(vectors.length, 37, "codes 0 to 35 and an unknown one");
  for (const v of vectors) {
    const code = Number(field(v, 0));
    assert.equal(errorName(code), field(v, 1), field(v, 0));
    assert.equal(isRetriable(code), field(v, 2) === "true", field(v, 0));
  }
  assert.equal(errorName(999), "UNKNOWN");
});

test("the error flag is checked before the typed body", () => {
  // A response with the ERROR flag whose body would be nonsense as a Produce response.
  let seen = 0;
  for (const v of lines("responses.hex")) {
    const frame = hex(field(v, 2));
    if (((frame[4] as number) & Flags.ERROR) === 0) {
      continue;
    }
    seen++;
    for (const key of [ApiKey.PRODUCE, ApiKey.FETCH, ApiKey.METADATA, ApiKey.DESCRIBE_METRICS]) {
      const r = decodeResponse(frame, key).response;
      assert.ok(r.type === "Error" || r.type === "Refused", field(v, 0));
    }
  }
  assert.ok(seen > 0);
  // Without the flag the same bytes are read as the typed body, which fails for the wrong key.
  const error = lines("responses.hex").find((v) => field(v, 0).endsWith("Error"));
  assert.ok(error !== undefined);
  const frame = hex(field(error, 2)).slice();
  frame[4] = 0;
  decodeFails(() => decodeResponse(frame, ApiKey.PRODUCE));
});

test("a refusal carries its reason", () => {
  let found = false;
  for (const v of lines("responses.hex")) {
    const r = decodeResponse(hex(field(v, 2)), Number(field(v, 1))).response;
    if (r.type === "Refused") {
      found = true;
      assert.equal(r.code, ErrorCode.UNAUTHORIZED);
      assert.equal(r.message, "no producer role for orders in shop");
    }
  }
  assert.ok(found);
});

test("tags 0 and 1 of a partition carry its pinned leader and placement (ADR 0121)", () => {
  let found = false;
  for (const v of lines("responses.hex")) {
    const r = decodeResponse(hex(field(v, 2)), Number(field(v, 1))).response;
    if (r.type === "Metadata") {
      for (const res of r.resources) {
        for (const p of res.partitions) {
          // Only partition 0 of "orders" has a pin (node 2) and a placement.
          const set = res.name === "orders" && p.partition === 0;
          found ||= set;
          assert.equal(p.pinnedLeader, set ? 2 : 0);
          assert.equal(p.placed, set);
        }
      }
    }
  }
  assert.ok(found);
});

test("tag 3 of a metrics answer is read into its queues (ADR 0120)", () => {
  let found = false;
  for (const v of lines("responses.hex")) {
    const r = decodeResponse(hex(field(v, 2)), Number(field(v, 1))).response;
    if (r.type === "DescribeMetrics" && r.queues.length > 0) {
      found = true;
      const q = r.queues[0]!;
      assert.deepEqual(q.readyByLevel, [2n, 1n, 1n]);
      assert.equal(q.readyLevelUnread, 1n);
      assert.equal(q.consumers[0]!.state, "active");
      assert.equal(q.consumers[1]!.state, "standby");
      assert.equal(q.consumers[1]!.peer, "10.0.0.9:40100");
      // Tags 4 to 6 (ADR 0122).
      const p = r.partitions[0]!;
      assert.equal(p.oldestTimestampMs, 1_763_000_000_000n);
      assert.equal(p.openTransactions, 1);
      assert.equal(q.consumers[0]!.inFlightBytes, 2048n);
      assert.equal(q.consumers[1]!.inFlightBytes, 0n);
      assert.deepEqual(r.disk, { freeLimitBytes: 1n << 30n, low: true });
    }
  }
  assert.ok(found);
});

test("leader hints are read where the tag block is", () => {
  let produce = false;
  let describe = false;
  for (const v of lines("responses.hex")) {
    const r = decodeResponse(hex(field(v, 2)), Number(field(v, 1))).response;
    if (r.type === "Produce") {
      for (const res of r.resources) {
        for (const p of res.partitions) {
          if (p.error === ErrorCode.NOT_LEADER) {
            assert.deepEqual(p.leaderHint, { leader: 2, term: 9n });
            produce = true;
          } else {
            assert.equal(p.leaderHint, null);
          }
        }
      }
    }
    if (r.type === "DescribeRoleBindings" && r.leaderHint !== null) {
      assert.deepEqual(r.leaderHint, { leader: 3, term: 9n });
      describe = true;
    }
  }
  assert.ok(produce && describe);
  assert.equal(isPush(hex(field(lines("responses.hex")[0] as string[], 2))), false);
});
