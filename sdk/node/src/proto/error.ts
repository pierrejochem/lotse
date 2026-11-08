import type { RequestHeader } from "./api-key.js";

export type DecodeKind = "TRUNCATED" | "MALFORMED" | "UNSUPPORTED";

/**
 * A frame that does not decode. A connection that meets one ends. `UNSUPPORTED` carries the request header when the
 * key or version is not served, so a node can answer it.
 */
export class DecodeError extends Error {
  readonly kind: DecodeKind;
  readonly header: RequestHeader | null;

  constructor(kind: DecodeKind, message: string, header: RequestHeader | null = null) {
    super(message);
    this.name = "DecodeError";
    this.kind = kind;
    this.header = header;
  }

  static truncated(): DecodeError {
    return new DecodeError("TRUNCATED", "frame is truncated");
  }

  static malformed(what: string): DecodeError {
    return new DecodeError("MALFORMED", `malformed frame: ${what}`);
  }
}
