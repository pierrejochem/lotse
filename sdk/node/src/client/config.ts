import type { Credentials } from "../auth/credentials.js";
import { type Address, parseAddress } from "../net/address.js";
import type { ConnectionOptions } from "../net/options.js";
import type { TlsConfig } from "../net/tls.js";
import { DEFAULT_TENANT } from "../proto/messages.js";

/**
 * How a `LotseClient` behaves. The defaults are the Rust client's: 10 s per request, 30 s of retrying, 50 ms between
 * attempts, 5 s for the leader to wait for the quorum on a produce, idempotent produce, tenant `default`. There is no
 * default for the transport: give `tls` or `plaintextOnLoopback: true`, or the client refuses to connect. Durations are
 * milliseconds.
 */
export interface ClientOptions {
  /** Brokers to ask for metadata first, each `host:port`; at least one. */
  readonly bootstrap: readonly string[];
  /** TLS 1.3 with the cluster's CA (and perhaps a client certificate). */
  readonly tls?: TlsConfig;
  /** Allows plain TCP to loopback addresses only; for development and tests. */
  readonly plaintextOnLoopback?: boolean;
  /** Logs in on every connection. Without it only a client certificate, if any, identifies the client. */
  readonly credentials?: Credentials;
  /** How long one request may take before it counts as lost. Default 10 000. */
  readonly requestTimeoutMs?: number;
  /** How long retriable failures (no leader, broken connections) are retried before `GaveUpError`. Default 30 000. */
  readonly retryForMs?: number;
  /** The pause between retries. Default 50. */
  readonly backoffMs?: number;
  /** How long the leader waits for the quorum on a produce. Default 5 000. */
  readonly produceTimeoutMs?: number;
  /**
   * Produce idempotently (the default). Needs the coordinator group, which every cluster started from a cluster file
   * has. Without it an unknown outcome is never retried, since a retry could write twice.
   */
  readonly idempotent?: boolean;
  /** How long a transaction may stay open before the coordinator aborts it. Sent with `InitProducer`. Default 60 000. */
  readonly txnTimeoutMs?: number;
  /**
   * A stable name for the idempotent producer (ADR 0071): the producer id comes from the coordinator under the name,
   * and every start raises its epoch, so a restarted client fences its earlier instance. Names share one namespace
   * with transactional ids; one client instance per name. Default none: a fresh id per client.
   */
  readonly producerName?: string;
  /**
   * The transactional id (ADR 0029), in the configured tenant (ADR 0062). With it `beginTransaction` works: the producer id
   * comes from the coordinator under the id, and a newer client with the same id fences this one. One client instance
   * per id. Default none.
   */
  readonly transactionalId?: string;
  /**
   * How long a transaction's publish uses the exchange's bindings it has read before it asks again. Default 30 000. A
   * `bind` or `unbind` by this client makes it ask again at once.
   */
  readonly metadataMaxAgeMs?: number;
  /** The tenant resources created by this client go to, and the one its producer names live in (ADR 0062). Default `default`. */
  readonly tenant?: string;
  /**
   * The zone this client runs in. With one, it fetches from a replica of the partition in that zone, which serves the
   * committed data it has, and from the leader if there is none (ADR 0104). Default none: fetches go to the leader.
   */
  readonly zone?: string;
  /** For the TCP connect and the TLS handshake together. Default 10 000. */
  readonly connectTimeoutMs?: number;
}

/** `ClientOptions` with every default filled in and every value checked. */
export interface ClientConfig {
  readonly bootstrap: readonly Address[];
  readonly requestTimeoutMs: number;
  readonly retryForMs: number;
  readonly backoffMs: number;
  readonly produceTimeoutMs: number;
  readonly idempotent: boolean;
  readonly txnTimeoutMs: number;
  readonly producerName: string | null;
  readonly transactionalId: string | null;
  readonly metadataMaxAgeMs: number;
  readonly tenant: string;
  readonly zone: string | null;
  readonly connection: ConnectionOptions;
}

function positive(name: string, v: number | undefined, fallback: number): number {
  const x = v ?? fallback;
  if (!Number.isFinite(x) || x <= 0) {
    throw new RangeError(`${name} must be positive, not ${x}`);
  }
  return x;
}

export function resolveClientOptions(o: ClientOptions): ClientConfig {
  if (o.bootstrap.length === 0) {
    throw new RangeError("a client needs at least one bootstrap address");
  }
  if (o.producerName !== undefined && (o.producerName.length === 0 || o.producerName.includes("\0"))) {
    throw new RangeError("a producer name is not empty and has no NUL");
  }
  if (o.transactionalId !== undefined && (o.transactionalId.length === 0 || o.transactionalId.includes("\0"))) {
    throw new RangeError("a transactional id is not empty and has no NUL");
  }
  const requestTimeoutMs = positive("requestTimeoutMs", o.requestTimeoutMs, 10_000);
  const connection: ConnectionOptions = {
    requestTimeoutMs,
    ...(o.connectTimeoutMs === undefined ? {} : { connectTimeoutMs: o.connectTimeoutMs }),
    ...(o.credentials === undefined ? {} : { credentials: o.credentials }),
    ...(o.tls !== undefined ? { tls: o.tls } : o.plaintextOnLoopback === true ? { plaintextOnLoopback: true } : {}),
  };
  return {
    bootstrap: o.bootstrap.map(parseAddress),
    requestTimeoutMs,
    retryForMs: positive("retryForMs", o.retryForMs, 30_000),
    backoffMs: positive("backoffMs", o.backoffMs, 50),
    produceTimeoutMs: positive("produceTimeoutMs", o.produceTimeoutMs, 5_000),
    idempotent: o.idempotent ?? true,
    txnTimeoutMs: positive("txnTimeoutMs", o.txnTimeoutMs, 60_000),
    producerName: o.producerName ?? null,
    transactionalId: o.transactionalId ?? null,
    metadataMaxAgeMs: positive("metadataMaxAgeMs", o.metadataMaxAgeMs, 30_000),
    tenant: o.tenant ?? DEFAULT_TENANT,
    zone: o.zone ?? null,
    connection,
  };
}
