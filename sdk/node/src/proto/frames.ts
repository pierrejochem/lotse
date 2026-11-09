import { ApiKey, Flags, isImplemented, type RequestHeader } from "./api-key.js";
import { DecodeError } from "./error.js";
import type { ErrorCodeValue } from "./error-code.js";
import * as m from "./messages.js";
import { Reader } from "./reader.js";
import { type Request, readRequest, requestApiKey, writeRequest } from "./request.js";
import { isErrorResponse, readResponse, type Response, writeResponse } from "./response.js";
import { Writer } from "./writer.js";

/** A frame is at most this long, length prefix excluded. A larger length ends the connection. */
export const MAX_FRAME = 16 * 1024 * 1024;

const UTF8_STRICT = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

// ---- length prefix ----

/** Prefixes a frame with its length, a big-endian `u32`: the one big-endian field of the protocol. */
export function withLengthPrefix(frame: Uint8Array): Uint8Array {
  if (frame.length > MAX_FRAME) {
    throw new RangeError(`frame of ${frame.length} bytes exceeds the limit`);
  }
  const out = new Uint8Array(4 + frame.length);
  new DataView(out.buffer).setUint32(0, frame.length, false);
  out.set(frame, 4);
  return out;
}

/** Reads the length from the first four bytes (big-endian). A length over `MAX_FRAME` throws; the connection ends. */
export function readLength(prefix: Uint8Array): number {
  if (prefix.length < 4) {
    throw DecodeError.truncated();
  }
  const len = new DataView(prefix.buffer, prefix.byteOffset, prefix.byteLength).getUint32(0, false);
  if (len > MAX_FRAME) {
    throw DecodeError.malformed(`frame of ${len} bytes exceeds the limit`);
  }
  return len;
}

// ---- requests ----

export interface DecodedRequest {
  readonly header: RequestHeader;
  readonly request: Request;
}

/** A request frame without its length prefix. */
export function encodeRequest(correlationId: number, request: Request): Uint8Array {
  const w = new Writer();
  w.u16(requestApiKey(request));
  w.u16(0);
  w.u32(correlationId);
  w.u16(0);
  w.noTags();
  writeRequest(w, request);
  return w.toBytes();
}

export function decodeRequest(frame: Uint8Array): DecodedRequest {
  const r = new Reader(frame);
  const header: RequestHeader = { apiKey: r.u16(), apiVersion: r.u16(), correlationId: r.u32(), flags: r.u16() };
  r.tags();
  if (!isImplemented(header.apiKey, header.apiVersion)) {
    throw new DecodeError(
      "UNSUPPORTED",
      `unsupported API key ${header.apiKey} version ${header.apiVersion}`,
      header,
    );
  }
  const request = readRequest(header.apiKey, r);
  r.finish();
  return { header, request };
}

// ---- responses ----

export interface DecodedResponse {
  readonly correlationId: number;
  readonly response: Response;
}

/** A response frame without its length prefix. The ERROR flag is set for `Error` and `Refused`. */
export function encodeResponse(correlationId: number, response: Response): Uint8Array {
  const w = new Writer();
  w.u32(correlationId);
  w.u16(isErrorResponse(response) ? Flags.ERROR : 0);
  w.noTags();
  writeResponse(w, response);
  return w.toBytes();
}

/** The correlation id of a response frame, without decoding the rest. */
export function responseCorrelationId(frame: Uint8Array): number {
  if (frame.length < 4) {
    throw DecodeError.truncated();
  }
  return new Reader(frame, 0, 4).u32();
}

/**
 * Decodes the answer to a request sent under `apiKey`. The ERROR flag is checked first: such a frame has the body
 * `error_code | tagged_fields` whatever the request was.
 */
export function decodeResponse(frame: Uint8Array, apiKey: number): DecodedResponse {
  const r = new Reader(frame);
  const correlationId = r.u32();
  const flags = r.u16();
  r.tags();
  if ((flags & Flags.ERROR) !== 0) {
    const code: ErrorCodeValue = r.u16();
    let reason: Uint8Array | null = null;
    for (const t of r.tags()) {
      if (t.tag === m.TAG_REASON) {
        reason = t.value;
        break;
      }
    }
    r.finish();
    if (reason === null) {
      return { correlationId, response: { type: "Error", code } };
    }
    let message: string;
    try {
      message = UTF8_STRICT.decode(reason);
    } catch {
      throw DecodeError.malformed("reason is not UTF-8");
    }
    return { correlationId, response: { type: "Refused", code, message } };
  }
  const response = readResponse(apiKey, r);
  r.finish();
  return { correlationId, response };
}

// ---- pushes ----

/** A queue delivery pushed to a subscription. An empty one with no error is an activation (ADR 0076). */
export interface DeliverPush {
  readonly subscriptionId: number;
  readonly error: ErrorCodeValue;
  /** One batch as stored. Only the records whose offsets are in `deliveries` are the consumer's. */
  readonly batch: Uint8Array;
  readonly deliveries: readonly m.DeliveredOffset[];
  readonly leaderHint: m.LeaderHint | null;
}

export function isActivation(push: DeliverPush): boolean {
  return push.error === 0 && push.batch.length === 0 && push.deliveries.length === 0;
}

export function encodePush(push: DeliverPush): Uint8Array {
  const w = new Writer();
  w.u32(0);
  w.u16(Flags.PUSH);
  w.noTags();
  w.u16(ApiKey.DELIVER);
  w.u16(0);
  w.u32(push.subscriptionId);
  w.u16(push.error);
  w.bytes(push.batch);
  w.array(push.deliveries, (w1, d) => w1.u64(d.offset).u16(d.deliveryCount));
  m.putHint(w, push.leaderHint);
  return w.toBytes();
}

/** A push has correlation id 0 and the PUSH flag. A response with id 0 cannot exist. */
export function isPush(frame: Uint8Array): boolean {
  return (
    frame.length >= 6 &&
    frame[0] === 0 &&
    frame[1] === 0 &&
    frame[2] === 0 &&
    frame[3] === 0 &&
    ((((frame[4] as number) | ((frame[5] as number) << 8)) & Flags.PUSH) !== 0)
  );
}

export function decodePush(frame: Uint8Array): DeliverPush {
  const r = new Reader(frame);
  const correlationId = r.u32();
  const flags = r.u16();
  if (correlationId !== 0 || (flags & Flags.PUSH) === 0) {
    throw DecodeError.malformed("not a push frame");
  }
  r.tags();
  if (r.u16() !== ApiKey.DELIVER || r.u16() !== 0) {
    throw DecodeError.malformed("unknown push");
  }
  const subscriptionId = r.u32();
  const error = r.u16();
  const batch = r.bytes();
  const deliveries = r.array(10, (r1) => ({ offset: r1.u64(), deliveryCount: r1.u16() }));
  const leaderHint = m.getHint(r);
  r.finish();
  return { subscriptionId, error, batch, deliveries, leaderHint };
}
