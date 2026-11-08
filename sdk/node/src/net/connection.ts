import { lookup } from "node:dns/promises";
import net from "node:net";
import tls from "node:tls";
import { authenticate } from "../auth/authenticator.js";
import type { Credentials } from "../auth/credentials.js";
import {
  type ApiVersionsResponse,
  DecodeError,
  decodePush,
  decodeResponse,
  type DeliverPush,
  encodeRequest,
  isPush,
  readLength,
  type Request,
  requestApiKey,
  type Response,
  responseCorrelationId,
  withLengthPrefix,
} from "../proto/index.js";
import { type Address, formatAddress } from "./address.js";
import { BrokerError, ConnectionError, InsecureTransportError, RequestTimeoutError } from "./errors.js";
import { type ConnectionOptions, type ResolvedOptions, resolveOptions } from "./options.js";
import type { TlsConfig } from "./tls.js";
import { TLS_PROTOCOL } from "./tls.js";

/** Subscriptions with pushes waiting for `register` that are kept; the one with the lowest id is dropped for more. */
export const ORPHAN_SUBSCRIPTIONS = 64;

/** Longest lifetime kept, so that arithmetic on it cannot overflow a sane clock: about 31 years. */
const MAX_LIFETIME_MS = 1_000_000_000_000n;
/** The longest delay `setTimeout` takes. A longer wait is slept in pieces. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/** Where a connection's session stands. */
export type SessionState =
  /** The session lasts as long as the connection (a client certificate, or no authentication required). */
  | "UNLIMITED"
  /** Valid, and not due for renewal yet. */
  | "ACTIVE"
  /** Valid, but past four fifths of its lifetime: authenticate again on this connection. */
  | "RENEW_DUE"
  /** Over. The node answers `UNAUTHORIZED`; open a new connection and authenticate. */
  | "ENDED";

/** Receives the pushes of one subscription. */
export interface PushListener {
  onPush(push: DeliverPush): void;
  /** The connection ended; no more pushes come. */
  onClose(reason: ConnectionError): void;
}

type ResponseOf<T extends Response["type"]> = Extract<Response, { type: T }>;

interface Slot {
  readonly key: number;
  readonly resolve: (r: Response) => void;
  readonly reject: (e: unknown) => void;
  timer: NodeJS.Timeout | undefined;
}

interface SessionTimes {
  readonly renewAt: bigint;
  readonly endsAt: bigint;
}

/** Whether `ip` (a literal address) is a loopback address, including IPv4-mapped IPv6 ones. */
export function isLoopbackAddress(ip: string): boolean {
  const v4 = /^(?:::ffff:)?(\d{1,3})\.\d{1,3}\.\d{1,3}\.\d{1,3}$/i.exec(ip);
  if (v4 !== null) {
    return v4[1] === "127";
  }
  const v6 = ip.toLowerCase();
  return v6 === "::1" || v6 === "0:0:0:0:0:0:0:1";
}

/** Collects what arrives on a socket and hands out whole frames. */
class FrameReader {
  private chunks: Buffer[] = [];
  private have = 0;

  push(chunk: Buffer): void {
    this.chunks.push(chunk);
    this.have += chunk.length;
  }

  private take(n: number): Buffer {
    const first = this.chunks[0];
    if (first !== undefined && first.length >= n) {
      const out = first.subarray(0, n);
      if (first.length === n) {
        this.chunks.shift();
      } else {
        this.chunks[0] = first.subarray(n);
      }
      this.have -= n;
      return out;
    }
    const out = Buffer.concat(this.chunks, this.have);
    const rest = out.subarray(n);
    this.chunks = rest.length === 0 ? [] : [rest];
    this.have = rest.length;
    return out.subarray(0, n);
  }

  /** The next whole frame without its length prefix, or `null` if it has not all arrived. Throws on a length over the limit. */
  next(): Buffer | null {
    if (this.have < 4) {
      return null;
    }
    const length = readLength(this.peek4());
    if (this.have < 4 + length) {
      return null;
    }
    this.take(4);
    return length === 0 ? Buffer.alloc(0) : this.take(length);
  }

  private peek4(): Buffer {
    const first = this.chunks[0];
    if (first !== undefined && first.length >= 4) {
      return first.subarray(0, 4);
    }
    return Buffer.concat(this.chunks, this.have).subarray(0, 4);
  }
}

function describe(e: unknown): string {
  return e instanceof Error ? e.message || e.name : String(e);
}

function dialTimeout(address: Address, ms: number): ConnectionError {
  return new ConnectionError(`cannot connect to ${formatAddress(address)}: no connection within ${ms} ms`, false);
}

/** Opens a socket to `target` and, for TLS, completes the handshake, checking the certificate against `address.host`. */
function dial(
  target: string,
  address: Address,
  tlsConfig: TlsConfig | undefined,
  timeoutMs: number,
): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let socket: net.Socket;
    const timer = setTimeout(() => {
      fail(dialTimeout(address, timeoutMs));
    }, timeoutMs);
    const fail = (e: unknown): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      reject(e);
    };
    const ok = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.setNoDelay(true);
      socket.setKeepAlive(true);
      resolve(socket);
    };
    if (tlsConfig === undefined) {
      socket = net.connect({ host: target, port: address.port });
      socket.once("connect", ok);
    } else {
      socket = tls.connect({
        host: target,
        port: address.port,
        // SNI takes a name, not an address.
        ...(net.isIP(address.host) === 0 ? { servername: address.host } : {}),
        ca: tlsConfig.ca,
        ...(tlsConfig.cert === undefined ? {} : { cert: tlsConfig.cert }),
        ...(tlsConfig.key === undefined ? {} : { key: tlsConfig.key }),
        minVersion: TLS_PROTOCOL,
        maxVersion: TLS_PROTOCOL,
        rejectUnauthorized: true,
        // The certificate must be valid for the host dialled, whichever of its addresses we connected to.
        checkServerIdentity: (_host, cert) => tls.checkServerIdentity(address.host, cert),
      });
      socket.once("secureConnect", ok);
    }
    socket.once("error", fail);
    socket.once("close", () => fail(new Error("the connection closed during the handshake")));
  });
}

/**
 * One persistent connection to one broker address.
 *
 * Requests are pipelined: `send` writes the request and returns a promise, and the response settles it whenever it
 * arrives, in any order, matched by correlation id. Correlation ids start at 1 and wrap past 0 (0 marks pushes).
 * Pushes go to the `PushListener` registered for their subscription; a push that arrives before its subscription is
 * registered is kept, for at most `ORPHAN_SUBSCRIPTIONS` subscriptions, and delivered on registration.
 *
 * A response with the `ERROR` flag rejects the promise with `BrokerError` before any typed decoding. A connection
 * that breaks, is closed, or receives something it cannot decode rejects every pending promise with
 * `ConnectionError`, and cannot be used again. Frames larger than `MAX_FRAME` end the connection. A connection whose
 * TLS peer certificate expires ends too (ADR 0068).
 *
 * Writes are handed to the socket whole and in call order; the client does not wait for the socket to drain, so a
 * caller that sends faster than the node reads should bound its requests in flight.
 */
export class Connection {
  private readonly slots = new Map<number, Slot>();
  private readonly subscriptions = new Map<number, PushListener>();
  /** Subscription id to pushes that came before `register`; the lowest id leaves first. */
  private readonly orphans = new Map<number, DeliverPush[]>();
  private readonly reader = new FrameReader();
  private lastId = 0;
  private broken: ConnectionError | null = null;
  private session: SessionTimes | null = null;
  private expiryTimer: NodeJS.Timeout | undefined;
  private readonly closedPromise: Promise<ConnectionError>;
  private resolveClosed!: (e: ConnectionError) => void;

  private constructor(
    readonly address: Address,
    private readonly socket: net.Socket,
    /** Whether the connection is encrypted with TLS. */
    readonly isSecure: boolean,
    private readonly options: ResolvedOptions,
  ) {
    this.closedPromise = new Promise((r) => {
      this.resolveClosed = r;
    });
    socket.on("data", (chunk: Buffer) => this.onData(chunk));
    socket.on("error", (e) => this.fail(new ConnectionError(`connection broke: ${describe(e)}`, true, e)));
    socket.on("close", () => this.fail(new ConnectionError("the broker closed the connection", true)));
    socket.on("end", () => this.fail(new ConnectionError("the broker closed the connection", true)));
    if (socket instanceof tls.TLSSocket) {
      this.armCertificateExpiry(socket);
    }
  }

  // ---- opening ----

  /**
   * Connects to `address` and, if the options carry credentials, authenticates.
   *
   * @throws InsecureTransportError if the options name no transport, or plain TCP is asked for an address that is not
   *   loopback; nothing is sent
   * @throws ConnectionError if the broker cannot be reached or the TLS handshake fails (the certificate is not signed
   *   by the given CA, or is not valid for the host)
   * @throws AuthenticationError if the broker does not accept the credentials
   */
  static async open(address: Address, options: ConnectionOptions): Promise<Connection> {
    const o = resolveOptions(options);
    const secure = o.tls !== undefined;
    if (!secure && !o.plaintextOnLoopback) {
      throw new InsecureTransportError(
        "the transport is not configured: give the options TLS (tls), or allow plain TCP on loopback (plaintextOnLoopback)",
      );
    }
    let resolved: { address: string }[];
    try {
      resolved = await lookup(address.host, { all: true });
    } catch (e) {
      throw new ConnectionError(`cannot resolve ${address.host}`, false, e);
    }
    if (resolved.length === 0) {
      throw new ConnectionError(`cannot resolve ${address.host}`, false);
    }
    if (!secure) {
      for (const r of resolved) {
        if (!isLoopbackAddress(r.address)) {
          throw new InsecureTransportError(
            `${formatAddress(address)} is not a loopback address; plain TCP is only allowed on loopback, configure TLS`,
          );
        }
      }
    }
    let connection: Connection | null = null;
    let last: unknown;
    for (const target of resolved) {
      try {
        const socket = await dial(target.address, address, o.tls, o.connectTimeoutMs);
        connection = new Connection(address, socket, secure, o);
        break;
      } catch (e) {
        last = e;
      }
    }
    if (connection === null) {
      if (last instanceof ConnectionError) {
        throw last;
      }
      throw new ConnectionError(`cannot connect to ${formatAddress(address)}: ${describe(last)}`, false, last);
    }
    if (o.credentials !== undefined) {
      try {
        await connection.authenticate(o.credentials);
      } catch (e) {
        connection.close();
        if (e instanceof ConnectionError) {
          // Nothing else went out on this connection.
          throw new ConnectionError(e.message, false, e);
        }
        throw e;
      }
    }
    return connection;
  }

  // ---- state ----

  /** Whether the connection is usable: not closed and not broken. */
  get isOpen(): boolean {
    return this.broken === null;
  }

  /** Settles, never rejecting, when the connection ends, with the reason. */
  get closed(): Promise<ConnectionError> {
    return this.closedPromise;
  }

  /** Where the session stands, by the options' ticker. */
  sessionState(): SessionState {
    const s = this.session;
    if (s === null) {
      return "UNLIMITED";
    }
    const now = this.options.ticker();
    if (now >= s.endsAt) {
      return "ENDED";
    }
    return now >= s.renewAt ? "RENEW_DUE" : "ACTIVE";
  }

  // ---- authentication ----

  /**
   * Authenticates this connection, or authenticates again before the session ends (the node insists on the same
   * principal). The session lifetime is taken from the last response, and counted from before the first request, so
   * the connection never believes in a session longer than the node does.
   *
   * @throws AuthenticationError if the node refuses
   * @throws ConnectionError if the connection fails meanwhile
   */
  async authenticate(credentials: Credentials): Promise<void> {
    const start = this.options.ticker();
    const lifetimeMs = await authenticate((r) => this.call(r), credentials, this.options.random);
    // u64 on the wire, held as a signed bit pattern: a negative value has the top bit set, far beyond any real lifetime.
    const clamped = lifetimeMs < 0n || lifetimeMs > MAX_LIFETIME_MS ? MAX_LIFETIME_MS : lifetimeMs;
    if (lifetimeMs === 0n) {
      this.session = null;
    } else {
      const nanos = clamped * 1_000_000n;
      this.session = { renewAt: start + (nanos / 5n) * 4n, endsAt: start + nanos };
    }
  }

  // ---- requests ----

  /**
   * Sends a request and returns the promise of its response. It rejects with `BrokerError` if the node refuses the
   * request as a whole, with `RequestTimeoutError` if no response comes within the request timeout, and with
   * `ConnectionError` if the connection fails first. `Credit` is never answered: use `sendOnly`.
   */
  send(request: Request): Promise<Response> {
    if (request.type === "Credit") {
      return Promise.reject(new TypeError("Credit is never answered; use sendOnly"));
    }
    const broken = this.broken;
    if (broken !== null) {
      return Promise.reject(notSent(broken));
    }
    const id = this.nextId();
    let wire: Uint8Array;
    try {
      wire = withLengthPrefix(encodeRequest(id, request));
    } catch (e) {
      return Promise.reject(new ConnectionError(`cannot encode the request: ${describe(e)}`, false, e));
    }
    return new Promise<Response>((resolve, reject) => {
      const slot: Slot = { key: requestApiKey(request), resolve, reject, timer: undefined };
      slot.timer = setTimeout(() => {
        if (this.slots.delete(id)) {
          reject(new RequestTimeoutError(this.options.requestTimeoutMs));
        }
      }, this.options.requestTimeoutMs);
      this.slots.set(id, slot);
      this.write(wire, (e) => this.fail(new ConnectionError(`send failed: ${describe(e)}`, true, e)));
    });
  }

  /**
   * Sends a request nobody waits for: `Credit`, which the broker never answers. An answer that comes anyway is
   * dropped.
   *
   * @throws ConnectionError if the connection is broken or closed; the request was not sent
   */
  sendOnly(request: Request): void {
    if (this.broken !== null) {
      throw notSent(this.broken);
    }
    const id = this.nextId();
    const wire = withLengthPrefix(encodeRequest(id, request));
    this.write(wire, (e) => this.fail(new ConnectionError(`send failed: ${describe(e)}`, true, e)));
  }

  /** Sends a request and waits for a response, optionally of the type `type`; another type is a `ConnectionError`. */
  async call<T extends Response["type"]>(request: Request, type: T): Promise<ResponseOf<T>>;
  async call(request: Request): Promise<Response>;
  async call(request: Request, type?: Response["type"]): Promise<Response> {
    const r = await this.send(request);
    if (type !== undefined && r.type !== type) {
      throw new ConnectionError(`expected ${type} but got ${r.type}`, true);
    }
    return r;
  }

  /** Asks the node for the API versions it supports. The client does not need the answer to work; it is there to look at. */
  apiVersions(): Promise<ApiVersionsResponse> {
    return this.call({ type: "ApiVersions" }, "ApiVersions");
  }

  private nextId(): number {
    // u32, never 0.
    this.lastId = (this.lastId + 1) % 0x1_0000_0000;
    if (this.lastId === 0) {
      this.lastId = 1;
    }
    return this.lastId;
  }

  private write(wire: Uint8Array, onError: (e: Error) => void): void {
    this.socket.write(wire, (e) => {
      if (e) {
        onError(e);
      }
    });
  }

  // ---- pushes ----

  /**
   * Registers the listener of subscription `subscriptionId`. Pushes that arrived earlier are delivered to it first, in
   * order, before this method returns; later ones follow.
   *
   * @throws ConnectionError if the connection is broken or closed
   */
  register(subscriptionId: number, listener: PushListener): void {
    if (this.broken !== null) {
      throw notSent(this.broken);
    }
    this.subscriptions.set(subscriptionId, listener);
    const early = this.orphans.get(subscriptionId);
    if (early !== undefined) {
      this.orphans.delete(subscriptionId);
      for (const push of early) {
        deliver(listener, push);
      }
    }
  }

  /** Stops delivering pushes of the subscription and forgets any that were kept for it. */
  unregister(subscriptionId: number): void {
    this.subscriptions.delete(subscriptionId);
    this.orphans.delete(subscriptionId);
  }

  /**
   * Registers subscription `subscriptionId` and returns its pushes as an async iterable. It ends when the
   * subscription is unregistered through `unregister` (call it, or `return()` on the iterator), and throws the
   * `ConnectionError` when the connection ends, after the pushes already received.
   */
  pushes(subscriptionId: number): AsyncIterable<DeliverPush> {
    const queue = new PushQueue(() => this.unregister(subscriptionId));
    this.register(subscriptionId, queue);
    return queue;
  }

  private route(push: DeliverPush): void {
    const listener = this.subscriptions.get(push.subscriptionId);
    if (listener !== undefined) {
      deliver(listener, push);
      return;
    }
    const kept = this.orphans.get(push.subscriptionId);
    if (kept !== undefined) {
      kept.push(push);
      return;
    }
    if (this.orphans.size >= ORPHAN_SUBSCRIPTIONS) {
      let lowest: number | undefined;
      for (const id of this.orphans.keys()) {
        if (lowest === undefined || id < lowest) {
          lowest = id;
        }
      }
      if (lowest !== undefined) {
        this.orphans.delete(lowest);
      }
    }
    this.orphans.set(push.subscriptionId, [push]);
  }

  // ---- reading ----

  private onData(chunk: Buffer): void {
    if (this.broken !== null) {
      return;
    }
    this.reader.push(chunk);
    try {
      for (let frame = this.reader.next(); frame !== null; frame = this.reader.next()) {
        this.handle(frame);
        if (this.broken !== null) {
          return;
        }
      }
    } catch (e) {
      if (e instanceof DecodeError) {
        this.fail(new ConnectionError(`undecodable frame: ${e.message}`, true, e));
      } else {
        this.fail(new ConnectionError(`reader failed: ${describe(e)}`, true, e));
      }
    }
  }

  private handle(frame: Uint8Array): void {
    if (isPush(frame)) {
      this.route(decodePush(frame));
      return;
    }
    const id = responseCorrelationId(frame);
    const slot = this.slots.get(id);
    if (slot === undefined) {
      // Late: the request timed out, or it was sent with sendOnly.
      return;
    }
    this.slots.delete(id);
    clearTimeout(slot.timer);
    let response: Response;
    try {
      response = decodeResponse(frame, slot.key).response;
    } catch (e) {
      if (e instanceof DecodeError) {
        slot.reject(new ConnectionError(`undecodable response: ${e.message}`, true, e));
      }
      throw e;
    }
    if (response.type === "Error") {
      slot.reject(new BrokerError(response.code, null));
    } else if (response.type === "Refused") {
      slot.reject(new BrokerError(response.code, response.message));
    } else {
      slot.resolve(response);
    }
  }

  // ---- certificate expiry (ADR 0068) ----

  private armCertificateExpiry(socket: tls.TLSSocket): void {
    const validTo = Date.parse(socket.getPeerCertificate().valid_to ?? "");
    if (Number.isNaN(validTo)) {
      return;
    }
    const wait = (): void => {
      const left = validTo - Date.now();
      if (left <= 0) {
        this.fail(new ConnectionError("the node's certificate expired", true));
        return;
      }
      this.expiryTimer = setTimeout(wait, Math.min(left, MAX_TIMER_MS));
      // A connection is kept alive by its socket, not by this deadline.
      this.expiryTimer.unref();
    };
    wait();
  }

  // ---- ending ----

  /** Closes the connection. Every pending request fails with `ConnectionError`. */
  close(): void {
    this.fail(new ConnectionError("connection closed", true));
  }

  private fail(why: ConnectionError): void {
    if (this.broken !== null) {
      return;
    }
    this.broken = why;
    const pending = [...this.slots.values()];
    this.slots.clear();
    const listeners = [...this.subscriptions.values()];
    this.subscriptions.clear();
    this.orphans.clear();
    clearTimeout(this.expiryTimer);
    this.socket.destroy();
    for (const slot of pending) {
      clearTimeout(slot.timer);
      slot.reject(why);
    }
    for (const listener of listeners) {
      try {
        listener.onClose(why);
      } catch (e) {
        process.emitWarning(`a push listener threw: ${describe(e)}`);
      }
    }
    this.resolveClosed(why);
  }

  toString(): string {
    return `Connection[${formatAddress(this.address)}, ${this.isSecure ? "TLS" : "plain"}${this.isOpen ? "" : ", closed"}]`;
  }
}

function notSent(why: ConnectionError): ConnectionError {
  return new ConnectionError(why.message, false, why);
}

function deliver(listener: PushListener, push: DeliverPush): void {
  try {
    listener.onPush(push);
  } catch (e) {
    process.emitWarning(`a push listener threw: ${describe(e)}`);
  }
}

/** A listener that is also an async iterable: pushes wait in a queue until they are read. */
class PushQueue implements PushListener, AsyncIterable<DeliverPush> {
  private readonly items: DeliverPush[] = [];
  private waiting: { resolve: (r: IteratorResult<DeliverPush>) => void; reject: (e: unknown) => void } | null = null;
  private ended = false;
  private failure: ConnectionError | null = null;

  constructor(private readonly unregister: () => void) {}

  onPush(push: DeliverPush): void {
    const w = this.waiting;
    if (w !== null) {
      this.waiting = null;
      w.resolve({ value: push, done: false });
    } else {
      this.items.push(push);
    }
  }

  onClose(reason: ConnectionError): void {
    this.failure = reason;
    this.finish();
  }

  private finish(): void {
    this.ended = true;
    const w = this.waiting;
    if (w !== null) {
      this.waiting = null;
      if (this.failure !== null) {
        w.reject(this.failure);
      } else {
        w.resolve({ value: undefined, done: true });
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<DeliverPush> {
    return {
      next: () => {
        const item = this.items.shift();
        if (item !== undefined) {
          return Promise.resolve({ value: item, done: false });
        }
        if (this.ended) {
          if (this.failure !== null) {
            const f = this.failure;
            this.failure = null;
            return Promise.reject(f);
          }
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise((resolve, reject) => {
          this.waiting = { resolve, reject };
        });
      },
      return: () => {
        this.unregister();
        this.items.length = 0;
        this.finish();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}
