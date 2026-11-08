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
  isImplemented,
  isPush,
  MAX_FRAME,
  NO_OFFSET,
  readLength,
  responseCorrelationId,
  withLengthPrefix,
  Writer,
} from "../src/proto/index.js";
import { writeResponse } from "../src/proto/response.js";

function decodeFails(f: () => unknown): DecodeError {
  try {
    f();
  } catch (e) {
    assert.ok(e instanceof DecodeError, String(e));
    return e;
  }
  assert.fail("expected a DecodeError");
}

test("the length prefix is big-endian and the rest little-endian", () => {
  const frame = encodeRequest(0x01020304, { type: "ApiVersions" });
  // api key 0, version 0, correlation 04 03 02 01 (little-endian), flags 0, empty tags, empty body tags
  assert.deepEqual([...frame], [0, 0, 0, 0, 4, 3, 2, 1, 0, 0, 0, 0]);
  const wire = withLengthPrefix(frame);
  assert.deepEqual([...wire.subarray(0, 4)], [0, 0, 0, 12]);
  assert.equal(readLength(wire), 12);
  assert.equal(readLength(new Uint8Array([0, 1, 2, 3])), 0x010203, "the first byte is the most significant");
  assert.equal(readLength(new Uint8Array([0, 0, 1, 0])), 256);
});

test("a length over the limit ends the connection", () => {
  assert.equal(readLength(new Uint8Array([1, 0, 0, 0])), MAX_FRAME);
  decodeFails(() => readLength(new Uint8Array([1, 0, 0, 1])));
  decodeFails(() => readLength(new Uint8Array([255, 255, 255, 255])));
  decodeFails(() => readLength(new Uint8Array([0, 0, 1])));
  assert.throws(() => withLengthPrefix(new Uint8Array(MAX_FRAME + 1)), RangeError);
});

test("an unknown key or version is reported with its header", () => {
  const frame = encodeRequest(77, { type: "ApiVersions" });
  frame[2] = 9; // version 9
  const e = decodeFails(() => decodeRequest(frame));
  assert.equal(e.kind, "UNSUPPORTED");
  assert.equal(e.header?.correlationId, 77);
  assert.equal(e.header?.apiVersion, 9);
  const other = encodeRequest(5, { type: "ApiVersions" });
  other[0] = 18; // a key the design leaves free
  assert.equal(decodeFails(() => decodeRequest(other)).kind, "UNSUPPORTED");
});

test("the node-internal keys are only constants", () => {
  for (const key of [
    ApiKey.SETTLE_VOTERS,
    ApiKey.DEAD_LETTER,
    ApiKey.VERIFY_TXN,
    ApiKey.CHECK_EMPTY,
    ApiKey.WRITE_TXN_MARKERS,
  ]) {
    assert.equal(isImplemented(key, 0), false);
    const frame = encodeRequest(1, { type: "ApiVersions" });
    frame[0] = key & 0xff;
    frame[1] = key >> 8;
    assert.equal(decodeFails(() => decodeRequest(frame)).kind, "UNSUPPORTED");
  }
  assert.equal(ApiKey.SETTLE_VOTERS, 43);
  assert.equal(ApiKey.DEAD_LETTER, 44);
  assert.equal(ApiKey.VERIFY_TXN, 45);
  assert.equal(ApiKey.CHECK_EMPTY, 46);
  assert.equal(ApiKey.WRITE_TXN_MARKERS, 1000);
});

test("unknown tags are skipped", () => {
  const w = new Writer();
  w.u32(1).u16(0);
  w.tags([{ tag: 42, value: new TextEncoder().encode("from the future") }]);
  writeResponse(w, { type: "Metadata", brokers: [], resources: [] });
  const r = decodeResponse(w.toBytes(), ApiKey.METADATA);
  assert.equal(r.correlationId, 1);
  assert.deepEqual(r.response, { type: "Metadata", brokers: [], resources: [] });
});

test("a push is told from a response", () => {
  const response = encodeResponse(5, { type: "Outcome", error: ErrorCode.NONE, leaderHint: null });
  assert.equal(isPush(response), false);
  assert.equal(isPush(new Uint8Array([0, 0, 0, 0, 2])), false);
  assert.equal(isPush(new Uint8Array([0, 0, 0, 0, 2, 0])), false, "ERROR is not PUSH");
  assert.equal(isPush(new Uint8Array([0, 0, 0, 0, 1, 0])), true);
  assert.equal(isPush(new Uint8Array([0, 0, 0, 0, 3, 0])), true);
  for (let i = 0; i < 4; i++) {
    const frame = new Uint8Array([0, 0, 0, 0, 1, 0]);
    frame[i] = 1;
    assert.equal(isPush(frame), false, `a nonzero correlation id is a response, byte ${i}`);
  }
  decodeFails(() => decodePush(response));
});

test("a push needs its flag and a zero correlation id", () => {
  const push = encodePush({ subscriptionId: 1, error: ErrorCode.NONE, batch: new Uint8Array(0), deliveries: [], leaderHint: null });
  assert.equal(decodePush(push).subscriptionId, 1);
  for (const flags of [0, 2]) {
    const bad = push.slice();
    bad[4] = flags;
    decodeFails(() => decodePush(bad));
  }
  const bad = push.slice();
  bad[0] = 1;
  decodeFails(() => decodePush(bad));
});

test("correlation ids use the whole u32 range", () => {
  const frame = encodeRequest(0xffff_ffff, { type: "WhoAmI" });
  assert.equal(decodeRequest(frame).header.correlationId, 0xffff_ffff);
  assert.throws(() => encodeRequest(2 ** 32, { type: "WhoAmI" }), RangeError);
  assert.equal(
    responseCorrelationId(encodeResponse(0xffff_ffff, { type: "Error", code: ErrorCode.UNAUTHORIZED })),
    0xffff_ffff,
  );
});

test("an error response is only a code and a tag block", () => {
  const frame = encodeResponse(9, { type: "Error", code: ErrorCode.UNSUPPORTED_VERSION });
  // correlation 9, flags ERROR, no tags, code 14, no tags
  assert.deepEqual([...frame], [9, 0, 0, 0, 2, 0, 0, 14, 0, 0]);
  assert.deepEqual(decodeResponse(frame, ApiKey.FETCH).response, { type: "Error", code: ErrorCode.UNSUPPORTED_VERSION });
});

test("u64 values keep their bits and no offset is all ones", () => {
  const frame = encodeResponse(1, {
    type: "OffsetFetch",
    resources: [
      {
        resourceId: 7n,
        partitions: [{ partition: 0, error: ErrorCode.NONE, offset: NO_OFFSET, leaderTerm: 0n, metadata: null }],
      },
    ],
  });
  const back = decodeResponse(frame, ApiKey.OFFSET_FETCH).response;
  assert.equal(back.type, "OffsetFetch");
  if (back.type === "OffsetFetch") {
    assert.equal(back.resources[0]?.partitions[0]?.offset, -1n);
    assert.equal(back.resources[0]?.partitions[0]?.metadata, null);
  }
  assert.equal(NO_OFFSET, -1n);
});

test("a produce with ids and offsets beyond 2^53 round-trips", () => {
  const big = 9007199254740993n;
  const frame = encodeRequest(1, {
    type: "AddPartitionsToTxn",
    txnId: "t",
    producerId: big,
    producerEpoch: 65535,
    resources: [{ resourceId: big + 2n, partitions: [0, 4294967295] }],
  });
  const back = decodeRequest(frame).request;
  assert.equal(back.type, "AddPartitionsToTxn");
  if (back.type === "AddPartitionsToTxn") {
    assert.equal(back.producerId, big);
    assert.equal(back.resources[0]?.resourceId, big + 2n);
    assert.deepEqual(back.resources[0]?.partitions, [0, 4294967295]);
    assert.equal(back.tenant, "default");
  }
});

test("the tenant is a tag, written only when it is not default", () => {
  const withTenant = encodeRequest(1, { type: "EndTxn", txnId: "t", producerId: 1n, producerEpoch: 0, commit: true, tenant: "shop" });
  const without = encodeRequest(1, { type: "EndTxn", txnId: "t", producerId: 1n, producerEpoch: 0, commit: true });
  assert.ok(withTenant.length > without.length);
  assert.equal(without[without.length - 1], 0, "an empty tag block");
  const back = decodeRequest(withTenant).request;
  assert.ok(back.type === "EndTxn" && back.tenant === "shop");
});
