import type { Credentials } from "../auth/credentials.js";
import type { TlsConfig } from "./tls.js";

/**
 * How connections are made: transport security, credentials, timeouts.
 *
 * There is no default for the transport. A connection is refused until `tls` or `plaintextOnLoopback` says which, so
 * that nothing is unencrypted by accident.
 */
export interface ConnectionOptions {
  /** TLS 1.3 with the cluster's CA. */
  readonly tls?: TlsConfig;
  /** Allows plain TCP, to loopback addresses only. For development nodes that were set up so. */
  readonly plaintextOnLoopback?: boolean;
  /** Authenticates every connection with SASL. A client certificate needs no entry: it is part of `tls`. */
  readonly credentials?: Credentials;
  /** Default 10 000. */
  readonly requestTimeoutMs?: number;
  /** For the TCP connect and the TLS handshake together. Default 10 000. */
  readonly connectTimeoutMs?: number;
  /** A monotonic clock in nanoseconds, for session lifetimes. Tests replace it. Default `process.hrtime.bigint`. */
  readonly ticker?: () => bigint;
  /** The source of the SCRAM client nonce. Default `crypto.randomBytes`. */
  readonly random?: (n: number) => Uint8Array;
}

export interface ResolvedOptions {
  readonly tls: TlsConfig | undefined;
  readonly plaintextOnLoopback: boolean;
  readonly credentials: Credentials | undefined;
  readonly requestTimeoutMs: number;
  readonly connectTimeoutMs: number;
  readonly ticker: () => bigint;
  readonly random: ((n: number) => Uint8Array) | undefined;
}

export function resolveOptions(o: ConnectionOptions): ResolvedOptions {
  const requestTimeoutMs = o.requestTimeoutMs ?? 10_000;
  const connectTimeoutMs = o.connectTimeoutMs ?? 10_000;
  for (const [name, v] of [
    ["requestTimeoutMs", requestTimeoutMs],
    ["connectTimeoutMs", connectTimeoutMs],
  ] as const) {
    if (!Number.isFinite(v) || v <= 0) {
      throw new RangeError(`${name} must be positive, not ${v}`);
    }
  }
  return {
    tls: o.tls,
    plaintextOnLoopback: o.plaintextOnLoopback ?? false,
    credentials: o.credentials,
    requestTimeoutMs,
    connectTimeoutMs,
    ticker: o.ticker ?? process.hrtime.bigint,
    random: o.random,
  };
}
