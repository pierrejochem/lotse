/** Why a batch or its records were refused. */
export type RecordFormatKind =
  | "TRUNCATED"
  | "VARINT_OVERFLOW"
  | "UNSUPPORTED_MAGIC"
  | "CHECKSUM"
  | "TOO_LARGE"
  | "MALFORMED"
  | "UNSUPPORTED_COMPRESSION";

/** A batch, a record or a varint that does not follow the format. `kind` says which check refused it. */
export class RecordFormatError extends Error {
  readonly kind: RecordFormatKind;

  constructor(kind: RecordFormatKind, message: string) {
    super(message);
    this.name = "RecordFormatError";
    this.kind = kind;
  }

  static truncated(): RecordFormatError {
    return new RecordFormatError("TRUNCATED", "buffer ends inside a batch");
  }

  static malformed(what: string): RecordFormatError {
    return new RecordFormatError("MALFORMED", `malformed batch: ${what}`);
  }
}
